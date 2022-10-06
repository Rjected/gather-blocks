use anvil_core::eth::{
    block::{Block, Header},
    transaction::TypedTransaction,
};
use clap::Parser;
use ethers::{
    prelude::{Middleware, U256},
    providers::{Provider, Ws},
    utils::rlp::{self, Decodable},
};
use fastrlp::Encodable;
use std::path::Path;
use tracing::info;
use tracing_subscriber::{prelude::*, EnvFilter};

pub mod efficient_file_writer;
use efficient_file_writer::EfficientFileWriter;

#[derive(Debug, Clone, Parser)]
#[clap(
    name = "gather-blocks",
    about = "This gathers historical blocks and headers from an archive node provider"
)]
pub struct RunArgs {
    /// The **websocket** provider URL (e.g. wss://localhost:8546)
    #[clap(short, long, env = "ETH_RPC_URL", value_name = "URL")]
    rpc_url: String,

    /// The directory to write the blocks to, with a default of ./blocks
    #[clap(short, long, default_value = "./blocks", value_name = "DIR")]
    directory: String,

    /// The number of blocks to write per file, with a default of 1000
    #[clap(short, long, default_value = "1000", value_name = "NUM")]
    blocks_per_file: usize,

    /// The block number to start at, with a default of 0
    #[clap(short, long, default_value = "0", value_name = "BLOCK_NUM")]
    start_block: u64,

    /// The block tag to end at, with a default of latest
    #[clap(short, long, default_value = "latest", value_name = "BLOCK_TAG")]
    end_block: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // parse CLI arguments
    let opts: RunArgs = RunArgs::parse();

    // set up tracing_subscriber
    let filter = if std::env::var(EnvFilter::DEFAULT_ENV)
        .unwrap_or_default()
        .is_empty()
    {
        EnvFilter::new("gather_blocks=info")
    } else {
        EnvFilter::from_default_env()
    };
    tracing_subscriber::registry()
        .with(tracing_subscriber::fmt::layer())
        .with(filter)
        .init();

    // initialize provider
    let ws = Ws::connect(&opts.rpc_url).await?;
    let provider = Provider::new(ws);

    // initialize the efficient file writer with 1000 blocks per file
    let mut writer = EfficientFileWriter::new(
        "blocks".to_string(),
        opts.blocks_per_file,
        Path::new(&opts.directory),
    )?;

    // using the end_block option, set the block number to end at
    let end_block = match opts.end_block.as_str() {
        "latest" => provider.get_block_number().await?,
        _ => opts.end_block.parse()?,
    }
    .as_u64();

    // get blocks up to the block number, saving them efficiently to disk with the efficient file writer. takes advantage of the writer's implementation of `Write`.
    for block_number in opts.start_block..=end_block {
        let block = provider.get_block_with_txs(block_number).await?;

        // panic if the block is None and return the proper cURL command to get the block from the archive node
        if block.is_none() {
            panic!("Block {} not found. Try running this command to get the block from the archive node: curl -X POST -H \"Content-Type: application/json\" --data '{{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"{}\", true],\"id\":1}}' {}", block_number, block_number, &opts.rpc_url);
        }
        let block = block.unwrap();

        // get a Vec<anvil_core::eth::Header> from the uncle hashes in the returned block
        let mut ommers: Vec<Header> = Vec::new();
        for (index, uncle_hash) in block.uncles.iter().enumerate() {
            // use the alchemy eth_getUncleByBlockNumberAndIndex method to get the uncle header
            let uncle = provider.get_uncle(block_number, index.into()).await?;

            if uncle.is_none() {
                // convert the websocket URL to an HTTP URL for the provider
                let http_url = opts.rpc_url.replace("ws", "http");
                panic!("Uncle {:?} not found. Try running this command to get the uncle from the archive node: curl -X POST -H \"Content-Type: application/json\" --data '{{\"jsonrpc\":\"2.0\",\"method\":\"eth_getUncleByBlockHashAndIndex\",\"params\":[\"{}\", \"{}\"],\"id\":1}}' {}", uncle_hash, block_number, index, &http_url);
            }
            let uncle = uncle.unwrap();

            // ethers has no direct conversion from a block to a header, so we have to do it manually
            let uncle_header = Header {
                parent_hash: uncle.parent_hash,
                ommers_hash: uncle.uncles_hash,
                beneficiary: uncle.author.unwrap(),
                state_root: uncle.state_root,
                transactions_root: uncle.transactions_root,
                receipts_root: uncle.receipts_root,
                logs_bloom: uncle.logs_bloom.unwrap(),
                difficulty: uncle.difficulty,
                number: U256::from(uncle.number.unwrap().as_u64()),
                gas_limit: uncle.gas_limit,
                gas_used: uncle.gas_used,
                timestamp: uncle.timestamp.as_u64(),
                extra_data: uncle.extra_data,
                mix_hash: uncle.mix_hash.unwrap(),
                nonce: uncle.nonce.unwrap(),
                base_fee_per_gas: uncle.base_fee_per_gas,
            };

            // finally append to the uncles vec
            ommers.push(uncle_header);
        }

        // create the block's header from the returned block
        let header = Header {
            parent_hash: block.parent_hash,
            ommers_hash: block.uncles_hash,
            beneficiary: block.author.unwrap(),
            state_root: block.state_root,
            transactions_root: block.transactions_root,
            receipts_root: block.receipts_root,
            logs_bloom: block.logs_bloom.unwrap(),
            difficulty: block.difficulty,
            number: U256::from(block.number.unwrap().as_u64()),
            gas_limit: block.gas_limit,
            gas_used: block.gas_used,
            timestamp: block.timestamp.as_u64(),
            extra_data: block.extra_data,
            mix_hash: block.mix_hash.unwrap(),
            nonce: block.nonce.unwrap(),
            base_fee_per_gas: block.base_fee_per_gas,
        };

        // create the block's transactions from the returned block
        let mut transactions: Vec<TypedTransaction> = Vec::new();
        for tx in block.transactions {
            // ethers does impl From<TypedTransaction> for Transaction, but not the other way
            // around, so we have to do it manually. let's encode the transaction to a byte array,
            // then decode the typed transaction from the byte array
            let tx_encoded = &tx.rlp().to_vec();
            let tx_rlp = rlp::Rlp::new(tx_encoded);
            let converted_transaction = TypedTransaction::decode(&tx_rlp).unwrap();

            // finally append to the transactions vec
            transactions.push(converted_transaction);
        }

        // now we have all the data we need to create the block
        let block = Block {
            header,
            ommers,
            transactions,
        };

        // encode the block to a byte array
        let mut block_encoded = Vec::new();
        block.encode(&mut block_encoded);

        // convert to a hex string
        let block_hex = hex::encode(block_encoded);

        // write the block to disk
        writer.write(block_hex)?;
        info!(
            "Wrote block {} with hash {:?}",
            block_number,
            block.header.hash()
        );
    }

    Ok(())
}
