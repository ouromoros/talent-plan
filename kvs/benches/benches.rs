use criterion::{black_box, criterion_group, criterion_main, Criterion, Bencher};
use kvs::{KvStore, SledStore};
use tempfile::TempDir;
use rand_chacha::rand_core::SeedableRng;
use rand::Rng;

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

criterion_group!(benches, bench_basic);
criterion_main!(benches);
