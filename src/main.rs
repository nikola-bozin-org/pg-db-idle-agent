use std::time::Duration;

use tokio::{task::JoinHandle, time};

pub struct PgDbIdleAgent{
    interval_secs:Duration
}

impl PgDbIdleAgent{
    pub fn new(interval_secs:Duration)->Self{
        Self {
            interval_secs
        }
    }

    pub fn start(&self)->JoinHandle<()>{
        let mut ticker = time::interval(self.interval_secs);
        tokio::task::spawn(async move {
            loop {
                ticker.tick().await;
                println!("Hello World")
            }
        })
    }
}


#[tokio::main]
async fn main() {
    let x = PgDbIdleAgent::new(Duration::from_secs(1));
    let handle = x.start();
    handle.await.unwrap() // Await the handle to prevent the main function from exiting prematurely
}
