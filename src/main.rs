use std::env;
use std::path::Path;
use std::time::Instant;

use byteorder::{BigEndian, ByteOrder};
use lmdb::{Cursor, Transaction};
use rand::{rngs::SmallRng, FromEntropy, Rng};

const TX_SIZE: usize = 5;
const MAX_VAL_SIZE: usize = 4096;

fn print_stat(env: &lmdb::Environment) {
    let stat = env.stat().unwrap();
    println!(
        "Stats depth={} branch_pages={} leaf_pages={} overflow_pages={} entries={}",
        stat.depth(),
        stat.branch_pages(),
        stat.leaf_pages(),
        stat.overflow_pages(),
        stat.entries(),
    );
}

fn initialize_value(len: usize) -> Vec<u8> {
    let mut v = Vec::new();
    for i in 0..len {
        v.push((i % 256) as u8);
    }
    v
}

fn verify_db(env: &lmdb::Environment, db: lmdb::Database, dump: bool) {
    let ro_txn = env.begin_ro_txn().unwrap();
    let mut cur = ro_txn.open_ro_cursor(db).unwrap();

    let mut expect_key: u64 = 1;
    let expect_val = initialize_value(MAX_VAL_SIZE);

    for (k, v) in cur.iter() {
        if dump {
            println!("{:?}: {:?}", BigEndian::read_u64(k), v);
        } else {
            assert_eq!(expect_key, BigEndian::read_u64(k));
            assert_eq!(expect_val[..v.len()].to_vec(), v);
        }
        expect_key += 1;
    }
}

fn get_last_key(env: &lmdb::Environment, db: lmdb::Database) -> Option<u64> {
    let ro_txn = env.begin_ro_txn().unwrap();
    let cur = ro_txn.open_ro_cursor(db).unwrap();

    match cur.get(None, None, lmdb_sys::MDB_LAST) {
        Err(_) => None,
        Ok((Some(key), _)) => Some(BigEndian::read_u64(key)),
        Ok((None, _)) => panic!("Unexpected"),
    }
}

fn main() -> lmdb::Result<()> {
    let mut rng = SmallRng::from_entropy();
    let tx_size: usize = TX_SIZE;
    let mut last_key: u64 = 0;
    let val = initialize_value(MAX_VAL_SIZE);

    let env_path = env::args().take(2).last().unwrap();
    let check = env::args().skip(2).next().map_or(false, |a| a == "check");
    let dump = env::args().skip(2).next().map_or(false, |a| a == "dump");

    let env = lmdb::Environment::new()
        .set_flags(lmdb::EnvironmentFlags::NO_SYNC)
        .set_map_size(2usize.pow(30) * 10) // 10GiB
        .open(Path::new(&env_path))?;
    let db = env.open_db(None)?;

    if dump {
        verify_db(&env, db, true);
        return Ok(());
    }

    // Verify existing keys
    print_stat(&env);
    println!("Verifying DB");
    verify_db(&env, db, false);
    println!("OK");
    match get_last_key(&env, db) {
        None => println!("Empty DB"),
        Some(last) => {
            last_key = last;
            println!("Last key in DB {}", last_key);
        }
    }
    if check {
        let ro_txn = env.begin_ro_txn()?;
        let mut key = [0; 8];
        BigEndian::write_u64(&mut key, last_key);
        ro_txn.get(db, &key)?;
        return Ok(());
    }

    // Delete existing keys
    'delete_loop: loop {
        let now = Instant::now();
        let mut rw_txn = env.begin_rw_txn()?;
        let mut key = [0; 8];
        for _ in 0..tx_size {
            if last_key == 0 {
                rw_txn.commit()?;
                println!("All keys deleted from DB");
                break 'delete_loop;
            }

            BigEndian::write_u64(&mut key, last_key);
            rw_txn.del(db, &key, None)?;

            last_key -= 1;
        }
        rw_txn.commit()?;

        if (last_key % 10000) == 0 {
            println!("Deleted last_key = {}", last_key);
            println!("Elapsed {:?}", now.elapsed());
        }
    }

    // Add keys again
    last_key = 1;
    loop {
        let now = Instant::now();
        let mut rw_txn = env.begin_rw_txn()?;
        let mut key = [0; 8];
        for _ in 0..tx_size {
            BigEndian::write_u64(&mut key, last_key);
            rw_txn.put(
                db,
                &key,
                &val[0..rng.gen_range(0, val.len())].to_vec(),
                lmdb::WriteFlags::empty(),
            )?;

            last_key += 1;
        }
        rw_txn.commit()?;

        if (last_key % 10000) == 1 {
            println!("Added last_key = {}", last_key);
            println!("Elapsed {:?}", now.elapsed());
        }
    }
}
