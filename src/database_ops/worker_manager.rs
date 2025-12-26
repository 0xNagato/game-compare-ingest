use anyhow::Result;
use futures::future::join_all;
use tracing::{error, info};

use crate::database_ops::db::Db;

#[async_trait::async_trait]
pub trait ProviderWorker: Send + Sync {
    fn name(&self) -> &'static str;
    async fn run(&self, db: &Db) -> Result<()>;
}

pub struct WorkerManager {
    db: Db,
}

impl WorkerManager {
    pub fn new(db: Db) -> Self {
        Self { db }
    }
    pub fn new_ref(db: &Db) -> Self {
        Self { db: db.clone() }
    }

    /// Run all workers concurrently, logging outcomes. Returns first error if any.
    pub async fn run_all(&self, workers: Vec<Box<dyn ProviderWorker>>) -> Result<()> {
        let mut tasks = Vec::with_capacity(workers.len());
        for w in workers {
            let db = self.db.clone();
            tasks.push(tokio::spawn(async move {
                info!(worker = w.name(), "starting worker");
                let res = w.run(&db).await;
                match &res {
                    Ok(_) => info!(worker = w.name(), "worker finished"),
                    Err(e) => error!(worker=w.name(), error=%e, "worker failed"),
                }
                res
            }));
        }
        let results = join_all(tasks).await;
        let mut first_err: Option<anyhow::Error> = None;
        for r in results {
            match r {
                Ok(Ok(_)) => {}
                Ok(Err(e)) => {
                    if first_err.is_none() {
                        first_err = Some(e);
                    }
                }
                Err(join_err) => {
                    if first_err.is_none() {
                        first_err = Some(anyhow::anyhow!(join_err));
                    }
                }
            }
        }
        if let Some(e) = first_err {
            Err(e)
        } else {
            Ok(())
        }
    }
}

// Simple adapters over provider modules to satisfy ProviderWorker
pub struct NexardaWorker;
#[async_trait::async_trait]
impl ProviderWorker for NexardaWorker {
    fn name(&self) -> &'static str {
        "nexarda"
    }
    async fn run(&self, db: &Db) -> Result<()> {
        super::nexarda::sync(db).await
    }
}
