use anyhow::Result;
use chrono::{DateTime, Datelike, TimeZone, Utc};
use tracing::instrument;

use crate::database_ops::db::{Db, PriceRow};

#[derive(Debug, Clone, Copy)]
pub struct MonthRange {
    pub start: DateTime<Utc>,
    pub end: DateTime<Utc>,
}

pub fn month_range(year: i32, month: u32) -> MonthRange {
    let start = Utc.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap();
    let (ny, nm) = if month == 12 {
        (year + 1, 1)
    } else {
        (year, month + 1)
    };
    let end = Utc.with_ymd_and_hms(ny, nm, 1, 0, 0, 0).unwrap();
    MonthRange { start, end }
}

#[instrument(skip(db, generator))]
pub async fn backfill_month<F>(db: &Db, year: i32, month: u32, mut generator: F) -> Result<usize>
where
    F: FnMut(MonthRange) -> Vec<PriceRow>,
{
    let range = month_range(year, month);
    let batch = generator(range);
    if batch.is_empty() {
        return Ok(0);
    }
    db.bulk_insert_prices(&batch).await?;
    Ok(batch.len())
}

#[instrument(skip(db, generator))]
pub async fn backfill_span<F>(
    db: &Db,
    start: MonthRange,
    end: MonthRange,
    mut generator: F,
) -> Result<usize>
where
    F: FnMut(MonthRange) -> Vec<PriceRow>,
{
    let mut total = 0usize;
    let mut y = start.start.year();
    let mut m = start.start.month();

    loop {
        let range = month_range(y, m);
        if range.start >= end.end {
            break;
        }
        let batch = generator(range);
        if !batch.is_empty() {
            db.bulk_insert_prices(&batch).await?;
            total += batch.len();
        }
        if m == 12 {
            y += 1;
            m = 1;
        } else {
            m += 1;
        }
    }
    Ok(total)
}
