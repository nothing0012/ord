use super::*;

pub(super) struct Flotsam {
  inscription_id: InscriptionId,
  offset: u64,
  origin: Origin,
}

enum Origin {
  New(u64),
  Old(SatPoint),
}

pub(super) struct InscriptionUpdater<'a, 'db, 'tx> {
  flotsam: Vec<Flotsam>,
  height: u64,
  id_to_satpoint: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, &'static SatPointValue>,
  value_receiver: &'a mut Receiver<u64>,
  id_to_entry: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, InscriptionEntryValue>,
  lost_sats: u64,
  next_number: u64,
  number_to_id: &'a mut Table<'db, 'tx, u64, &'static InscriptionIdValue>,
  outpoint_to_value: &'a mut Table<'db, 'tx, &'static OutPointValue, u64>,
  reward: u64,
  sat_to_inscription_id: &'a mut Table<'db, 'tx, u64, &'static InscriptionIdValue>,
  satpoint_to_id: &'a mut Table<'db, 'tx, &'static SatPointValue, &'static InscriptionIdValue>,
  timestamp: u32,
  value_cache: &'a mut HashMap<OutPoint, u64>,
}

impl<'a, 'db, 'tx> InscriptionUpdater<'a, 'db, 'tx> {
  pub(super) fn new(
    height: u64,
    id_to_satpoint: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, &'static SatPointValue>,
    value_receiver: &'a mut Receiver<u64>,
    id_to_entry: &'a mut Table<'db, 'tx, &'static InscriptionIdValue, InscriptionEntryValue>,
    lost_sats: u64,
    number_to_id: &'a mut Table<'db, 'tx, u64, &'static InscriptionIdValue>,
    outpoint_to_value: &'a mut Table<'db, 'tx, &'static OutPointValue, u64>,
    sat_to_inscription_id: &'a mut Table<'db, 'tx, u64, &'static InscriptionIdValue>,
    satpoint_to_id: &'a mut Table<'db, 'tx, &'static SatPointValue, &'static InscriptionIdValue>,
    timestamp: u32,
    value_cache: &'a mut HashMap<OutPoint, u64>,
  ) -> Result<Self> {
    let next_number = number_to_id
      .iter()?
      .rev()
      .map(|(number, _id)| number.value() + 1)
      .next()
      .unwrap_or(0);

    Ok(Self {
      flotsam: Vec::new(),
      height,
      id_to_satpoint,
      value_receiver,
      id_to_entry,
      lost_sats,
      next_number,
      number_to_id,
      outpoint_to_value,
      reward: Height(height).subsidy(),
      sat_to_inscription_id,
      satpoint_to_id,
      timestamp,
      value_cache,
    })
  }

  pub(super) fn index_transaction_inscriptions(
    &mut self,
    tx: &Transaction,
    txid: Txid,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
  ) -> Result<u64> {
    let mut inscriptions = Vec::new();

    let mut input_value = 0;
    for tx_in in &tx.input {
      if tx_in.previous_output.is_null() {
        input_value += Height(self.height).subsidy();
      } else {
        for (old_satpoint, inscription_id) in
          Index::inscriptions_on_output(self.satpoint_to_id, tx_in.previous_output)?
        {
          inscriptions.push(Flotsam {
            offset: input_value + old_satpoint.offset,
            inscription_id,
            origin: Origin::Old(old_satpoint),
          });
        }

        input_value += if let Some(value) = self.value_cache.remove(&tx_in.previous_output) {
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
        }
      }
    }

    if inscriptions.iter().all(|flotsam| flotsam.offset != 0)
      && Inscription::from_transaction(tx).is_some()
    {
      inscriptions.push(Flotsam {
        inscription_id: txid.into(),
        offset: 0,
        origin: Origin::New(input_value - tx.output.iter().map(|txout| txout.value).sum::<u64>()),
      });
    };

    let is_coinbase = tx
      .input
      .first()
      .map(|tx_in| tx_in.previous_output.is_null())
      .unwrap_or_default();

    if is_coinbase {
      inscriptions.append(&mut self.flotsam);
    }

    inscriptions.sort_by_key(|flotsam| flotsam.offset);
    let mut inscriptions = inscriptions.into_iter().peekable();

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

        self.update_inscription_location(
          input_sat_ranges,
          inscriptions.next().unwrap(),
          new_satpoint,
          tx,
        )?;
      }

      output_value = end;

      self.value_cache.insert(
        OutPoint {
          vout: vout.try_into().unwrap(),
          txid,
        },
        tx_out.value,
      );
    }

    if is_coinbase {
      for flotsam in inscriptions {
        let new_satpoint = SatPoint {
          outpoint: OutPoint::null(),
          offset: self.lost_sats + flotsam.offset - output_value,
        };
        self.update_inscription_location(input_sat_ranges, flotsam, new_satpoint, tx)?;
      }

      Ok(self.reward - output_value)
    } else {
      self.flotsam.extend(inscriptions.map(|flotsam| Flotsam {
        offset: self.reward + flotsam.offset - output_value,
        ..flotsam
      }));
      self.reward += input_value - output_value;
      Ok(0)
    }
  }

  fn update_inscription_location(
    &mut self,
    input_sat_ranges: Option<&VecDeque<(u64, u64)>>,
    flotsam: Flotsam,
    new_satpoint: SatPoint,
    #[allow(unused_variables)] tx: &Transaction,
  ) -> Result {
    let inscription_id = flotsam.inscription_id.store();

    match flotsam.origin {
      Origin::Old(old_satpoint) => {
        self.satpoint_to_id.remove(&old_satpoint.store())?;

        #[cfg(feature = "kafka")]
        {
          use self::stream::StreamEvent;
          StreamEvent::new(
            tx,
            flotsam.inscription_id,
            new_satpoint,
            self.timestamp,
            self.height,
          )
          .with_transfer(old_satpoint)
          .publish()?;
        }
      }
      Origin::New(fee) => {
        self
          .number_to_id
          .insert(&self.next_number, &inscription_id)?;

        let mut sat = None;
        if let Some(input_sat_ranges) = input_sat_ranges {
          let mut offset = 0;
          for (start, end) in input_sat_ranges {
            let size = end - start;
            if offset + size > flotsam.offset {
              let n = start + flotsam.offset - offset;
              self.sat_to_inscription_id.insert(&n, &inscription_id)?;
              sat = Some(Sat(n));
              break;
            }
            offset += size;
          }
        }

        self.id_to_entry.insert(
          &inscription_id,
          &InscriptionEntry {
            fee,
            height: self.height,
            number: self.next_number,
            sat,
            timestamp: self.timestamp,
          }
          .store(),
        )?;

        #[cfg(feature = "kafka")]
        {
          use self::stream::StreamEvent;
          StreamEvent::new(
            tx,
            flotsam.inscription_id,
            new_satpoint,
            self.timestamp,
            self.height,
          )
          .with_create(tx, sat, self.next_number)
          .publish()?;
        }

        self.next_number += 1;
      }
    }

    let new_satpoint = new_satpoint.store();

    self.satpoint_to_id.insert(&new_satpoint, &inscription_id)?;
    self.id_to_satpoint.insert(&inscription_id, &new_satpoint)?;

    Ok(())
  }
}

#[cfg(feature = "kafka")]
mod stream {
  use crate::subcommand::traits::Output;

  use super::*;
  use base64::encode;
  use rdkafka::{
    config::FromClientConfig,
    producer::{BaseProducer, BaseRecord},
    ClientConfig,
  };
  use std::env;

  lazy_static! {
    static ref CLIENT: StreamClient = StreamClient::new();
  }

  struct StreamClient {
    producer: BaseProducer,
    topic: String,
  }

  impl StreamClient {
    fn new() -> Self {
      StreamClient {
        producer: BaseProducer::from_config(
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

  #[derive(Serialize)]
  pub struct StreamEvent {
    // common fields
    inscription_id: InscriptionId,
    tx_value: u64,
    tx_id: String,
    new_location: SatPoint,
    new_owner: Option<Address>,
    new_output_value: u64,

    block_timestamp: u32,
    block_height: u64,

    // create fields
    sat: Option<Sat>,
    sat_details: Option<Output>, // Output is borrowed from subcommand::traits::Output, to show the details of the sat
    inscription_number: Option<u64>,
    content_type: Option<String>,
    content_length: Option<usize>,
    content_media: Option<String>,
    content_body: Option<String>,

    // transfer fields
    old_location: Option<SatPoint>,
  }

  impl StreamEvent {
    pub fn new(
      tx: &Transaction,
      inscription_id: InscriptionId,
      new_satpoint: SatPoint,
      block_timestamp: u32,
      block_height: u64,
    ) -> Self {
      StreamEvent {
        inscription_id,
        new_location: new_satpoint,
        block_timestamp,
        block_height,
        new_owner: Some(
          Address::from_script(
            &tx
              .output
              .get(new_satpoint.outpoint.vout as usize)
              .unwrap_or(&TxOut::default())
              .script_pubkey,
            StreamEvent::get_network(),
          )
          .unwrap_or(Address::p2sh(&Script::default(), StreamEvent::get_network()).unwrap()),
        ),
        new_output_value: tx
          .output
          .get(new_satpoint.outpoint.vout as usize)
          .unwrap_or(&TxOut {
            value: 0,
            script_pubkey: Script::new(),
          })
          .value,
        tx_value: tx.output.iter().map(|txout: &TxOut| txout.value).sum(),
        tx_id: tx.txid().to_string(),
        sat: None,
        inscription_number: None,
        content_type: None,
        content_length: None,
        content_media: None,
        content_body: None,
        old_location: None,
        sat_details: None,
      }
    }

    fn get_network() -> Network {
      Network::from_str(&env::var("NETWORK").unwrap_or("bitcoin".to_owned())).unwrap()
    }

    pub fn with_transfer(&mut self, old_satpoint: SatPoint) -> &mut Self {
      self.old_location = Some(old_satpoint);
      self
    }

    pub fn with_create(
      &mut self,
      tx: &Transaction,
      sat: Option<Sat>,
      inscription_number: u64,
    ) -> &mut Self {
      let inscription = Inscription::from_transaction(tx).unwrap();

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
          if body.len() < kafka_body_max_bytes {
            Some(encode::<&[u8]>(body))
          } else {
            None
          }
        }
        None => None,
      };

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

    pub fn publish(&self) -> Result {
      let key = self.inscription_id.to_string();
      let payload = serde_json::to_vec(&self)?;
      let record = BaseRecord::to(&CLIENT.topic).key(&key).payload(&payload);
      match CLIENT.producer.send(record) {
        Ok(_) => Ok(()),
        Err((e, _)) => Err(anyhow!("failed to send kafka message: {}", e)),
      }
    }
  }
}
