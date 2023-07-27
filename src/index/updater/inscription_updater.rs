use self::stream::StreamEvent;
use {super::*, inscription::Curse};

#[derive(Debug, Clone)]
pub(super) struct Flotsam {
  inscription_id: InscriptionId,
  offset: u64,
  origin: Origin,
}

#[derive(Debug, Clone)]
enum Origin {
  New {
    cursed: bool,
    fee: u64,
    parent: Option<InscriptionId>,
    pointer: Option<u64>,
    unbound: bool,
    inscription: Inscription,
  },
  Old {
    old_satpoint: SatPoint,
  },
}

pub(super) struct InscriptionUpdater<'a, 'db, 'tx> {
  flotsam: Vec<Flotsam>,
  height: u64,
  id_to_children:
    &'a mut MultimapTable<'db, 'tx, &'static InscriptionIdValue, &'static InscriptionIdValue>,
  id_to_satpoint: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, &'static SatPointValue>,
  value_receiver: &'a mut Receiver<u64>,
  id_to_entry: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, InscriptionEntryValue>,
  pub(super) lost_sats: u64,
  pub(super) cursed_inscription_count: u64,
  pub(super) blessed_inscription_count: u64,
  pub(super) next_sequence_number: u64,
  inscription_number_to_id: &'a mut Table<'db, 'tx, i64, &'static InscriptionIdValue>,
  sequence_number_to_id: &'a mut Table<'db, 'tx, u64, &'static InscriptionIdValue>,
  outpoint_to_value: &'a mut Table<'db, 'tx, &'static OutPointValue, u64>,
  reward: u64,
  sat_to_inscription_id: &'a mut MultimapTable<'db, 'tx, u64, &'static InscriptionIdValue>,
  satpoint_to_id:
    &'a mut MultimapTable<'db, 'tx, &'static SatPointValue, &'static InscriptionIdValue>,
  timestamp: u32,
  pub(super) unbound_inscriptions: u64,
  block_hash: BlockHash,
  value_cache: &'a mut HashMap<OutPoint, u64>,
}

impl<'a, 'db, 'tx> InscriptionUpdater<'a, 'db, 'tx> {
  pub(super) fn new(
    height: u64,
    id_to_children: &'a mut MultimapTable<
      'db,
      'tx,
      &'static InscriptionIdValue,
      &'static InscriptionIdValue,
    >,
    id_to_satpoint: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, &'static SatPointValue>,
    value_receiver: &'a mut Receiver<u64>,
    id_to_entry: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, InscriptionEntryValue>,
    lost_sats: u64,
    inscription_number_to_id: &'a mut Table<'db, 'tx, i64, &'static InscriptionIdValue>,
    cursed_inscription_count: u64,
    blessed_inscription_count: u64,
    sequence_number_to_id: &'a mut Table<'db, 'tx, u64, &'static InscriptionIdValue>,
    outpoint_to_value: &'a mut Table<'db, 'tx, &'static OutPointValue, u64>,
    sat_to_inscription_id: &'a mut MultimapTable<'db, 'tx, u64, &'static InscriptionIdValue>,
    satpoint_to_id: &'a mut MultimapTable<
      'db,
      'tx,
      &'static SatPointValue,
      &'static InscriptionIdValue,
    >,
    timestamp: u32,
    unbound_inscriptions: u64,
    block_hash: BlockHash,
    value_cache: &'a mut HashMap<OutPoint, u64>,
  ) -> Result<Self> {
    let next_sequence_number = sequence_number_to_id
      .iter()?
      .next_back()
      .and_then(|result| result.ok())
      .map(|(number, _id)| number.value() + 1)
      .unwrap_or(0);

    Ok(Self {
      flotsam: Vec::new(),
      height,
      id_to_children,
      id_to_satpoint,
      value_receiver,
      id_to_entry,
      lost_sats,
      cursed_inscription_count,
      blessed_inscription_count,
      next_sequence_number,
      sequence_number_to_id,
      inscription_number_to_id,
      outpoint_to_value,
      reward: Height(height).subsidy(),
      sat_to_inscription_id,
      satpoint_to_id,
      timestamp,
      unbound_inscriptions,
      block_hash,
      value_cache,
    })
  }

  pub(super) fn index_envelopes(
    &mut self,
    tx: &Transaction,
    txid: Txid,
    tx_block_index: usize,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    index: &Index,
  ) -> Result {
    let mut envelopes = ParsedEnvelope::from_transaction(tx).into_iter().peekable();
    let mut floating_inscriptions = Vec::new();
    let mut inscribed_offsets = BTreeMap::new();
    let mut total_input_value = 0;
    let mut id_counter = 0;

    for (input_index, tx_in) in tx.input.iter().enumerate() {
      // skip subsidy since no inscriptions possible
      if tx_in.previous_output.is_null() {
        total_input_value += Height(self.height).subsidy();
        continue;
      }

      // find existing inscriptions on input (transfers of inscriptions)
      for (old_satpoint, inscription_id) in Index::inscriptions_on_output_ordered(
        self.id_to_entry,
        self.satpoint_to_id,
        tx_in.previous_output,
      )? {
        let offset = total_input_value + old_satpoint.offset;
        floating_inscriptions.push(Flotsam {
          offset,
          inscription_id,
          origin: Origin::Old { old_satpoint },
        });

        inscribed_offsets
          .entry(offset)
          .and_modify(|(_id, count)| *count += 1)
          .or_insert((inscription_id, 0));
      }

      let offset = total_input_value;

      // multi-level cache for UTXO set to get to the input amount
      let current_input_value = if let Some(value) = self.value_cache.remove(&tx_in.previous_output)
      {
        value
      } else if let Some(value) = self
        .outpoint_to_value
        .remove(&tx_in.previous_output.store())?
      {
        value.value()
      } else {
        self.value_receiver.blocking_recv().ok_or_else(|| {
          anyhow!(
            "failed to get transaction for {}",
            tx_in.previous_output.txid
          )
        })?
      };

      total_input_value += current_input_value;

      // go through all inscriptions in this input
      while let Some(inscription) = envelopes.peek() {
        if inscription.input != u32::try_from(input_index).unwrap() {
          break;
        }

        let inscription_id = InscriptionId {
          txid,
          index: id_counter,
        };

        let curse = if inscription.payload.unrecognized_even_field {
          Some(Curse::UnrecognizedEvenField)
        } else if inscription.payload.duplicate_field {
          Some(Curse::DuplicateField)
        } else if inscription.payload.incomplete_field {
          Some(Curse::IncompleteField)
        } else if inscription.input != 0 {
          Some(Curse::NotInFirstInput)
        } else if inscription.offset != 0 {
          Some(Curse::NotAtOffsetZero)
        } else if inscription.payload.pointer.is_some() {
          Some(Curse::Pointer)
        } else if inscription.pushnum {
          Some(Curse::Pushnum)
        } else if inscribed_offsets.contains_key(&offset) {
          let seq_num = self.id_to_entry.len()?;

          let sat = Self::calculate_sat(input_sat_ranges, offset);

          log::info!("processing reinscription {inscription_id} on sat {:?}: sequence number {seq_num}, inscribed offsets {:?}", sat, inscribed_offsets);

          Some(Curse::Reinscription)
        } else {
          None
        };

        if curse.is_some() {
          log::info!("found cursed inscription {inscription_id}: {:?}", curse);
        }

        let cursed = if let Some(Curse::Reinscription) = curse {
          let first_reinscription = inscribed_offsets
            .get(&offset)
            .map(|(_id, count)| count == &0)
            .unwrap_or(false);

          let initial_inscription_is_cursed = inscribed_offsets
            .get(&offset)
            .and_then(|(inscription_id, _count)| {
              match self.id_to_entry.get(&inscription_id.store()) {
                Ok(option) => option.map(|entry| {
                  let loaded_entry = InscriptionEntry::load(entry.value());
                  loaded_entry.inscription_number < 0
                }),
                Err(_) => None,
              }
            })
            .unwrap_or(false);

          log::info!("{inscription_id}: is first reinscription: {first_reinscription}, initial inscription is cursed: {initial_inscription_is_cursed}");

          !(initial_inscription_is_cursed && first_reinscription)
        } else {
          curse.is_some()
        };

        let unbound = current_input_value == 0 || curse == Some(Curse::UnrecognizedEvenField);

        if curse.is_some() || unbound {
          log::info!(
            "indexing inscription {inscription_id} with curse {:?} as cursed {} and unbound {}",
            curse,
            cursed,
            unbound
          );
        }

        floating_inscriptions.push(Flotsam {
          inscription_id,
          offset,
          origin: Origin::New {
            cursed,
            fee: 0,
            parent: inscription.payload.parent(),
            pointer: inscription.payload.pointer(),
            unbound,
            inscription: inscription.clone().payload,
          },
        });

        envelopes.next();
        id_counter += 1;
      }
    }

    let potential_parents = floating_inscriptions
      .iter()
      .map(|flotsam| flotsam.inscription_id)
      .collect::<HashSet<InscriptionId>>();

    for flotsam in &mut floating_inscriptions {
      if let Flotsam {
        origin: Origin::New { parent, .. },
        ..
      } = flotsam
      {
        if let Some(purported_parent) = parent {
          if !potential_parents.contains(purported_parent) {
            *parent = None;
          }
        }
      }
    }

    // still have to normalize over inscription size
    let total_output_value = tx.output.iter().map(|txout| txout.value).sum::<u64>();
    let mut floating_inscriptions = floating_inscriptions
      .into_iter()
      .map(|flotsam| {
        if let Flotsam {
          inscription_id,
          offset,
          origin:
            Origin::New {
              cursed,
              fee: _,
              parent,
              pointer,
              unbound,
              inscription,
            },
        } = flotsam
        {
          Flotsam {
            inscription_id,
            offset,
            origin: Origin::New {
              fee: (total_input_value - total_output_value) / u64::from(id_counter),
              cursed,
              parent,
              pointer,
              unbound,
              inscription,
            },
          }
        } else {
          flotsam
        }
      })
      .collect::<Vec<Flotsam>>();

    let is_coinbase = tx
      .input
      .first()
      .map(|tx_in| tx_in.previous_output.is_null())
      .unwrap_or_default();

    if is_coinbase {
      floating_inscriptions.append(&mut self.flotsam);
    }

    floating_inscriptions.sort_by_key(|flotsam| flotsam.offset);
    let mut inscriptions = floating_inscriptions.into_iter().peekable();

    let mut range_to_vout = BTreeMap::new();
    let mut new_locations = Vec::new();
    let mut output_value = 0;
    for (vout, tx_out) in tx.output.iter().enumerate() {
      let end = output_value + tx_out.value;

      while let Some(flotsam) = inscriptions.peek() {
        if flotsam.offset >= end {
          break;
        }

        let new_satpoint = SatPoint {
          outpoint: OutPoint {
            txid,
            vout: vout.try_into().unwrap(),
          },
          offset: flotsam.offset - output_value,
        };

        new_locations.push((new_satpoint, inscriptions.next().unwrap()));
      }

      range_to_vout.insert((output_value, end), vout.try_into().unwrap());

      output_value = end;

      self.value_cache.insert(
        OutPoint {
          vout: vout.try_into().unwrap(),
          txid,
        },
        tx_out.value,
      );
    }

    for (new_satpoint, mut flotsam) in new_locations.into_iter() {
      let new_satpoint = match flotsam.origin {
        Origin::New {
          pointer: Some(pointer),
          ..
        } if pointer < output_value => {
          match range_to_vout.iter().find_map(|((start, end), vout)| {
            (pointer >= *start && pointer < *end).then(|| (vout, pointer - start))
          }) {
            Some((vout, offset)) => {
              flotsam.offset = pointer;
              SatPoint {
                outpoint: OutPoint { txid, vout: *vout },
                offset,
              }
            }
            _ => new_satpoint,
          }
        }
        _ => new_satpoint,
      };

      self.update_inscription_location(input_sat_ranges, flotsam, new_satpoint, tx, tx_block_index, index)?;
    }

    if is_coinbase {
      for flotsam in inscriptions {
        let new_satpoint = SatPoint {
          outpoint: OutPoint::null(),
          offset: self.lost_sats + flotsam.offset - output_value,
        };
        self.update_inscription_location(
          input_sat_ranges,
          flotsam,
          new_satpoint,
          tx,
          tx_block_index,
          index,
        )?;
      }
      self.lost_sats += self.reward - output_value;
      Ok(())
    } else {
      self.flotsam.extend(inscriptions.map(|flotsam| Flotsam {
        offset: self.reward + flotsam.offset - output_value,
        ..flotsam
      }));
      self.reward += total_input_value - output_value;
      Ok(())
    }
  }

  fn calculate_sat(
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    input_offset: u64,
  ) -> Option<Sat> {
    let mut sat = None;
    if let Some(input_sat_ranges) = input_sat_ranges {
      let mut offset = 0;
      for (start, end) in input_sat_ranges {
        let size = end - start;
        if offset + size > input_offset {
          let n = start + input_offset - offset;
          sat = Some(Sat(n));
          break;
        }
        offset += size;
      }
    }
    sat
  }

  fn update_inscription_location(
    &mut self,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    flotsam: Flotsam,
    new_satpoint: SatPoint,
    tx: &Transaction,
    tx_block_index: usize,
    index: &Index,
  ) -> Result {
    let inscription_id = flotsam.inscription_id.store();
    let unbound = match flotsam.origin {
      Origin::Old { old_satpoint } => {
        StreamEvent::new(
          tx,
          tx_block_index,
          flotsam.inscription_id,
          new_satpoint,
          self.timestamp,
          self.height,
          self.block_hash,
        )
        .with_transfer(old_satpoint, index)
        .publish()?;

        self.satpoint_to_id.remove_all(&old_satpoint.store())?;
        false
      }
      Origin::New {
        cursed,
        fee,
        parent,
        unbound,
        inscription,
        ..
      } => {
        let inscription_number = if cursed {
          let number: i64 = self.cursed_inscription_count.try_into().unwrap();
          self.cursed_inscription_count += 1;

          // because cursed numbers start at -1
          -(number + 1)
        } else {
          let number: i64 = self.blessed_inscription_count.try_into().unwrap();
          self.blessed_inscription_count += 1;

          number
        };

        self
          .inscription_number_to_id
          .insert(inscription_number, &inscription_id)?;

        let sequence_number = self.next_sequence_number;
        self.next_sequence_number += 1;

        self
          .sequence_number_to_id
          .insert(sequence_number, &inscription_id)?;

        let sat = if unbound {
          None
        } else {
          Self::calculate_sat(input_sat_ranges, flotsam.offset)
        };

        if let Some(Sat(n)) = sat {
          self.sat_to_inscription_id.insert(&n, &inscription_id)?;
        }

        self.id_to_entry.insert(
          &inscription_id,
          &InscriptionEntry {
            fee,
            height: self.height,
            inscription_number,
            sequence_number,
            parent,
            sat,
            timestamp: self.timestamp,
          }
          .store(),
        )?;

        if let Some(parent) = parent {
          self
            .id_to_children
            .insert(&parent.store(), &inscription_id)?;
        }
        StreamEvent::new(
          tx,
          tx_block_index,
          flotsam.inscription_id,
          match unbound {
            true => SatPoint {
              outpoint: unbound_outpoint(),
              offset: self.unbound_inscriptions,
            },
            false => new_satpoint,
          },
          self.timestamp,
          self.height,
          self.block_hash,
        )
        .with_create(sat, inscription_number, inscription)
        .publish()?;

        unbound
      }
    };

    let satpoint = if unbound {
      let new_unbound_satpoint = SatPoint {
        outpoint: unbound_outpoint(),
        offset: self.unbound_inscriptions,
      };
      self.unbound_inscriptions += 1;
      new_unbound_satpoint.store()
    } else {
      new_satpoint.store()
    };

    self.satpoint_to_id.insert(&satpoint, &inscription_id)?;
    self.id_to_satpoint.insert(&inscription_id, &satpoint)?;

    Ok(())
  }
}

mod stream {
  use crate::subcommand::traits::Output;
  use base64::{engine::general_purpose, Engine as _};

  use super::*;
  use rdkafka::{
    config::FromClientConfig,
    producer::{BaseRecord, DefaultProducerContext, ThreadedProducer},
    ClientConfig,
  };
  use std::env;
  use std::str::FromStr;

  lazy_static! {
    static ref CLIENT: StreamClient = StreamClient::new();
  }

  struct StreamClient {
    producer: ThreadedProducer<DefaultProducerContext>,
    topic: String,
  }

  impl StreamClient {
    fn new() -> Self {
      StreamClient {
        producer: ThreadedProducer::from_config(
          ClientConfig::new()
            .set(
              "bootstrap.servers",
              env::var("KAFKA_BOOTSTRAP_SERVERS").unwrap_or("localhost:9092".to_owned()),
            )
            .set(
              "message.timeout.ms",
              env::var("KAFKA_MESSAGE_TIMEOUT_MS").unwrap_or("5000".to_owned()),
            )
            .set(
              "client.id",
              env::var("KAFKA_CLIENT_ID").unwrap_or("ord-producer".to_owned()),
            ),
        )
        .expect("failed to create kafka producer"),
        topic: env::var("KAFKA_TOPIC").unwrap_or("ord-stream".to_owned()),
      }
    }
  }

  #[derive(Serialize, Deserialize)]
  pub struct BRC20 {
    p: String,
    op: String,
    tick: String,
    max: Option<String>,
    lim: Option<String>,
    amt: Option<String>,
    dec: Option<String>,
  }

  #[derive(Serialize, Deserialize)]
  pub struct Domain {
    p: String,
    op: String,
    name: String,
  }

  impl Domain {
    pub fn parse(body: &[u8]) -> Option<Self> {
      if let Ok(name) = Self::validate_string(body) {
        return Some(Domain {
          name,
          p: "sns".to_owned(),
          op: "reg".to_owned(),
        });
      }

      if let Ok(data) = serde_json::from_slice::<Domain>(body) {
        if data.p != "sns" || data.op != "reg" {
          return None;
        }
        if Self::validate_string(data.name.as_bytes()).is_ok() {
          return Some(data);
        }
      }

      None
    }

    pub fn validate_string(input: &[u8]) -> Result<String, &'static str> {
      // Convert &[u8] to &str
      let str_input = match std::str::from_utf8(input) {
        Ok(v) => v,
        Err(_) => return Err("Invalid UTF-8 data"),
      };

      // Turn the string into lowercase
      let mut lower = str_input.to_lowercase();

      // Delete everything after the first whitespace or newline (\n)
      if let Some(end) = lower.find(|c: char| c.is_whitespace()) {
        lower.truncate(end);
      }

      // Trim all whitespace and newlines
      let trimmed = lower.trim();

      // Validate that there is only one period (.) in the name
      let period_count = trimmed.matches('.').count();
      if period_count != 1 {
        return Err("There should be exactly one period (.) in the name");
      }

      if trimmed.ends_with('}') {
        return Err("The name should not end with a curly brace (})");
      }

      Ok(trimmed.to_string())
    }
  }

  #[derive(Serialize)]
  pub struct StreamEvent {
    version: String,

    // common fields
    inscription_id: InscriptionId,
    new_location: SatPoint,
    new_owner: Option<Address>,
    new_output_value: u64,

    tx_id: String,
    tx_value: u64,
    tx_block_index: usize,
    tx_is_coinbase: bool,
    block_timestamp: u32,
    block_height: u64,
    block_hash: BlockHash,

    // create fields
    sat: Option<Sat>,
    sat_details: Option<Output>, // Output is borrowed from subcommand::traits::Output, to show the details of the sat
    inscription_number: Option<i64>,
    content_type: Option<String>,
    content_length: Option<usize>,
    content_media: Option<String>,
    content_body: Option<String>,

    // plugins
    brc20: Option<BRC20>,
    domain: Option<Domain>,

    // transfer fields
    old_location: Option<SatPoint>,
    old_owner: Option<Address>,
  }

  impl StreamEvent {
    pub fn new(
      tx: &Transaction,
      tx_block_index: usize,
      inscription_id: InscriptionId,
      new_satpoint: SatPoint,
      block_timestamp: u32,
      block_height: u64,
      block_hash: BlockHash,
    ) -> Self {
      StreamEvent {
        version: "6.0.0".to_owned(), // should match the ord-kafka docker image version
        inscription_id,
        block_timestamp,
        block_height,
        block_hash,
        new_location: new_satpoint,
        new_owner: Some(
          Address::from_script(
            &tx
              .output
              .get(new_satpoint.outpoint.vout as usize)
              .unwrap_or(&TxOut::default())
              .script_pubkey,
            StreamEvent::get_network(),
          )
          .unwrap_or(Address::p2sh(&ScriptBuf::default(), StreamEvent::get_network()).unwrap()),
        ),
        new_output_value: tx
          .output
          .get(new_satpoint.outpoint.vout as usize)
          .unwrap_or(&TxOut {
            value: 0,
            script_pubkey: ScriptBuf::new(),
          })
          .value,
        tx_value: tx.output.iter().map(|txout: &TxOut| txout.value).sum(),
        tx_id: tx.txid().to_string(),
        tx_block_index,
        tx_is_coinbase: tx.is_coin_base(),
        sat: None,
        inscription_number: None,
        content_type: None,
        content_length: None,
        content_media: None,
        content_body: None,
        brc20: None,
        domain: None,
        old_location: None,
        old_owner: None,
        sat_details: None,
      }
    }

    fn key(&self) -> String {
      if let Ok(kafka_topic) = env::var("KAFKA_TOPIC") {
        if !kafka_topic.to_lowercase().contains("brc20") {
          return self.inscription_id.to_string();
        }
      }

      if let Some(brc20) = &self.brc20 {
        return brc20.tick.clone().to_lowercase();
      }
      self.inscription_id.to_string()
    }

    fn get_network() -> Network {
      Network::from_str(&env::var("NETWORK").unwrap_or("bitcoin".to_owned())).unwrap()
    }

    fn enrich_content(&mut self, inscription: Inscription) -> &mut Self {
      self.content_type = inscription
        .content_type()
        .map(|content_type| content_type.to_string());
      self.content_length = inscription.content_length();
      self.content_media = Some(inscription.media().to_string());
      self.content_body = match inscription.body() {
        Some(body) => {
          // only encode if the body length is less than 1M bytes
          let kafka_body_max_bytes = env::var("KAFKA_BODY_MAX_BYTES")
            .unwrap_or("950000".to_owned())
            .parse::<usize>()
            .unwrap();

          // Text Media and Content-Type starting with "text/" are included with the content-body payload
          let is_text = inscription.media() == Media::Text
            || self
              .content_type
              .clone()
              .map(|ct| ct.starts_with("text/") || ct.starts_with("image/svg"))
              .unwrap_or(false);

          if is_text && body.len() < kafka_body_max_bytes {
            self.brc20 = serde_json::from_slice(body).unwrap_or(None);
            self.domain = Domain::parse(body);
            Some(general_purpose::STANDARD.encode(body))
          } else {
            None
          }
        }
        None => None,
      };
      self
    }

    pub(crate) fn with_transfer(&mut self, old_satpoint: SatPoint, index: &Index) -> &mut Self {
      self.old_location = Some(old_satpoint);
      if let Some(inscription) = index
        .get_inscription_by_id_unsafe(self.inscription_id)
        .unwrap_or_else(|_| panic!("Inscription should exist: {}", self.inscription_id))
      {
        self.enrich_content(inscription);
        self.old_owner = index
          .get_transaction(old_satpoint.outpoint.txid)
          .unwrap_or(None)
          .and_then(|tx| {
            tx.output
              .get(old_satpoint.outpoint.vout as usize)
              .and_then(|txout| {
                Address::from_script(&txout.script_pubkey, StreamEvent::get_network())
                  .map_err(|e| {
                    log::error!(
                      "StreamEvent::with_transfer could not parse old_owner address: {}",
                      e
                    );
                  })
                  .ok()
              })
          });
      };
      self
    }

    pub(crate) fn with_create(
      &mut self,
      sat: Option<Sat>,
      inscription_number: i64,
      inscription: Inscription,
    ) -> &mut Self {
      self.enrich_content(inscription);
      self.sat = sat;
      self.inscription_number = Some(inscription_number);
      self.sat_details = match self.sat {
        Some(Sat(n)) => {
          let sat = Sat(n);
          Some(Output {
            number: sat.n(),
            decimal: sat.decimal().to_string(),
            degree: sat.degree().to_string(),
            name: sat.name(),
            height: sat.height().0,
            cycle: sat.cycle(),
            epoch: sat.epoch().0,
            period: sat.period(),
            offset: sat.third(),
            rarity: sat.rarity(),
          })
        }
        None => None,
      };
      self
    }

    pub fn publish(&mut self) -> Result {
      if env::var("KAFKA_TOPIC").is_err() {
        return Ok(());
      }
      let key = self.key();
      let payload = serde_json::to_vec(&self)?;
      let record = BaseRecord::to(&CLIENT.topic).key(&key).payload(&payload);
      match CLIENT.producer.send(record) {
        Ok(_) => Ok(()),
        Err((e, _)) => Err(anyhow!("failed to send kafka message: {}", e)),
      }?;
      println!("{}", serde_json::to_string(&self)?);
      Ok(())
    }
  }
}
