pub mod queries;

fn main() {
    println!("Hello, world!");

    let begin = std::time::Instant::now();
    let df = queries::q5()
        .unwrap()
        .set_policy_checking(false)
        .collect()
        .unwrap();
    let end = std::time::Instant::now();


    println!("{df}");
    println!("time used: {:?}", end - begin);
}
