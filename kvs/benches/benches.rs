use criterion::{criterion_group, criterion_main, Criterion, Bencher, BenchmarkId};
use kvs::{KvStore, SledStore, KvsEngine};
use tempfile::TempDir;
use rand_chacha::rand_core::SeedableRng;
use rand::Rng;
use kvs::thread_pool::{ThreadPool, SharedQueueThreadPool};
use std::net::TcpListener;
use std::thread::{spawn, sleep};
use kvs::client::Client;
use std::sync::Arc;
use failure::_core::time::Duration;

fn get_random_value(r: &mut rand_chacha::ChaCha8Rng) -> String {
    let n: u32 = r.gen_range(1..=100000);
    let s: String = rand::thread_rng()
        .sample_iter(&rand::distributions::Alphanumeric)
        .take(n as usize)
        .map(char::from)
        .collect();
    s
}

fn bench_engine_writes<E: kvs::KvsEngine>(b: &mut Bencher, engine: &mut E) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut r = rand_chacha::ChaCha8Rng::seed_from_u64(42);
    for _ in 0..100 {
        keys.push(get_random_value(&mut r));
        values.push(get_random_value(&mut r));
    }
    b.iter(|| {
        for i in 0..100 {
            let k = keys[i].clone();
            let v = values[i].clone();
            engine.set(k, v).unwrap();
        }
    })
}

fn bench_engine_reads<E: kvs::KvsEngine>(b: &mut Bencher, engine: &mut E) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut r = rand_chacha::ChaCha8Rng::seed_from_u64(42);
    for _ in 0..100 {
        keys.push(get_random_value(&mut r));
        values.push(get_random_value(&mut r));
    }
    for i in 0..100 {
        let k = keys[i].clone();
        let v = values[i].clone();
        engine.set(k, v).unwrap();
    }
    b.iter(|| {
        for i in 0..1000 {
            let k = keys[i % 100].clone();
            engine.get(k).unwrap();
        }
    })
}

pub fn bench_basic(c: &mut Criterion) {
    c.bench_function("kvs reads", |b| {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        bench_engine_reads(b, &mut store);
    });
    c.bench_function("kvs writes", |b| {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = KvStore::open(temp_dir.path()).unwrap();
        bench_engine_writes(b, &mut store);
    });
    c.bench_function("sled reads", |b| {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = SledStore::open(temp_dir.path()).unwrap();
        bench_engine_reads(b, &mut store);
    });
    c.bench_function("sled writes", |b| {
        let temp_dir = TempDir::new().expect("unable to create temporary working directory");
        let mut store = SledStore::open(temp_dir.path()).unwrap();
        bench_engine_writes(b, &mut store);
    });
}

fn bench_server_reads<E: KvsEngine, T: ThreadPool>(b: &mut Bencher, engine: E, pool: T) {
    let mut keys = Vec::new();
    let mut values = Vec::new();
    let mut r = rand_chacha::ChaCha8Rng::seed_from_u64(42);
    for _ in 0..100 {
        keys.push(get_random_value(&mut r));
        values.push(get_random_value(&mut r));
    }
    for i in 0..100 {
        let k = keys[i].clone();
        let v = values[i].clone();
        engine.set(k, v).unwrap();
    }

    let listener = TcpListener::bind("127.0.0.1:5000").unwrap();
    let mut server = kvs::server::Server::new(pool, engine, listener);
    spawn(move || server.run());

    let client_pool = SharedQueueThreadPool::new(10).unwrap();
    let keys = Arc::new(keys);
    b.iter(|| {
        let (s, r) = std::sync::mpsc::channel::<()>();
        for _ in 0..10 {
            let keys = keys.clone();
            let s = s.clone();
            client_pool.spawn(move || {
                let mut client = Client::new("127.0.0.1:5000").unwrap();
                for i in 0..1 {
                    client.get(keys[0].to_string()).expect("get error");
                }
                s.send(()).expect("send error");
            })
        }
        for _ in 0..10 {
            r.recv().unwrap();
        }
    });
    let mut client = Client::new("127.0.0.1:5000").unwrap();
    client.shutdown().unwrap();
    sleep(Duration::from_secs(1));
}

pub fn bench_threads(c: &mut Criterion) {
    let mut group = c.benchmark_group("read_shared");
    for num_core in [1, 2, 4, 8, 16].iter() {
        group.bench_with_input(BenchmarkId::from_parameter(num_core), num_core, |b, &num_core| {
            let temp_dir = TempDir::new().expect("unable to create temporary working directory");
            let store = KvStore::open(temp_dir.path()).unwrap();
            let pool = SharedQueueThreadPool::new(num_core).unwrap();
            bench_server_reads(b, store, pool);
        });
    }
    group.finish();
}

criterion_group!(benches, bench_basic, bench_threads);
criterion_main!(benches);
