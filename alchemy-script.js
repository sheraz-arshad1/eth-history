const mariadb = require("mariadb");
const Web3 = require("web3");
const dotenv = require("dotenv");

dotenv.config();
const web3 = new Web3(process.env.RPC_URL);

const big = web3.utils.toBN;
const toHex = web3.utils.toHex;

const EXTERNAL = "external";
const INTERNAL = "internal";
const TOKEN = "erc20";
const DEFAULT_UUID = "DEFAULT_UUID";

const ASSET_MOVEMENT = ["In", "Out"];
const ASSET_TYPES = ["ETH", "ERC20"];

const {
    MARIADB_HOST,
    MARIADB_USER,
    MARIADB_PASSWORD,
    MARIADB_DATABASE,
    MARIADB_CONNECTION_LIMIT,
} = process.env;

const pool = mariadb.createPool({
    host: MARIADB_HOST,
    user: MARIADB_USER,
    password: MARIADB_PASSWORD,
    database: MARIADB_DATABASE,
    connectionLimit: MARIADB_CONNECTION_LIMIT
});

main();

async function main() {
    const accounts = [
        "0x5e624faedc7aa381b574c3c2ff1731677dd2ee1d",
        "0xaf648ffbc940570f3f6a9ca49b07ba7bc520bcdf",
    ];

    const toBlock = await web3.eth.getBlockNumber();
    const toBlockHex = toHex(toBlock).toString();
    console.log(toBlock)

    const accountsAndLastFetchedBlocks = await getLastFetchedBlockForAccounts(accounts);
    const data = await processAccounts(accountsAndLastFetchedBlocks, toBlockHex);
    console.log(JSON.stringify(data))
    try {
        await populateDatabase(data);
        await updateLatestFetchedBlockForAccounts(accounts, toBlock);
        process.exit(0);
    } catch (e) {
        console.error(e.message);
        process.exit(1);
    }
}

async function processAccounts(accountsAndLastFetchedBlocks, toBlock) {
    return Object.keys(accountsAndLastFetchedBlocks).reduce(
        async (
            acc,
            account,
        ) => {
            return new Promise(
                async (
                    resolve,
                    _
                ) => {
                    const [
                        processedTransfersFrom,
                        processedTransfersTo
                    ] = await Promise.all([
                        new Promise(async (resolve, _) => {
                                let uuid = DEFAULT_UUID;
                                let processedTransfersFrom = {};
                                while (uuid) {
                                    const {
                                        result: {
                                            transfers: transfersFrom,
                                            pageKey
                                        }
                                    } = await fetchTransfers({
                                        fromBlock: accountsAndLastFetchedBlocks[account],
                                        toBlock,
                                        fromAddress: account,
                                        uuid,
                                    });

                                    uuid = pageKey;
                                    processedTransfersFrom = {
                                        ...processedTransfersFrom,
                                        ...(await processTransfers(transfersFrom, true))
                                    };
                                }
                                resolve(processedTransfersFrom);
                            }
                        ),
                        new Promise(async (resolve, _) => {
                            let uuid = DEFAULT_UUID;
                            let processedTransfersTo = {};
                            while (uuid) {
                                const {
                                    result: {
                                        transfers: transfersTo,
                                        pageKey,
                                    }
                                } = await fetchTransfers({
                                    fromBlock: accountsAndLastFetchedBlocks[account],
                                    toBlock,
                                    toAddress: account,
                                    uuid,
                                });

                                uuid = pageKey;
                                processedTransfersTo = {
                                    ...processedTransfersTo,
                                    ...(await processTransfers(transfersTo)),
                                }
                            }
                            resolve(processedTransfersTo);
                        })
                    ]);

                    acc = await acc;
                    acc[account] = {
                        Out: {
                            ETH: processedTransfersFrom.ETH,
                            ERC20: processedTransfersFrom.ERC20,
                        },
                        In: {
                            ETH: processedTransfersTo.ETH,
                            ERC20: processedTransfersTo.ERC20,
                        }
                    };
                    resolve(acc);
                });

        },
        Promise.resolve({})
    );
}

function processTransfers(transfers, includeGasCost = false) {
    return transfers.reduce(
        async (
            acc,
            {
                blockNum,
                hash: txHash,
                to,
                from,
                asset: name,
                category,
                rawContract: {
                    value,
                    address,
                    decimal,
                }
            }
        ) => {
            if (
                category === EXTERNAL
                || category === INTERNAL
                || category === TOKEN
            ) {
                console.log("TxHash: ", txHash);

                const transferData = {
                    txHash,
                    from,
                    to,
                    name,
                    value: big(value).toString(),
                    address: category === TOKEN
                        ? address
                        : "none",
                    decimal: !!decimal
                        ? big(decimal).toString()
                        : "none",
                    blockNum: big(blockNum).toString(),
                    timestamp: (await web3.eth.getBlock(big(blockNum))).timestamp,
                };

                if (includeGasCost) {
                    const {
                        from: txSender,
                        gasUsed,
                        effectiveGasPrice
                    } = await web3.eth.getTransactionReceipt(txHash);
                    if (
                        txSender === from
                        && category !== TOKEN
                    ) {
                        const gasPriceToUse = effectiveGasPrice
                            ? effectiveGasPrice
                            : (await web3.eth.getTransaction(txHash)).gasPrice;

                        transferData.gasCostInWei = big(gasUsed)
                            .mul(big(gasPriceToUse))
                            .toString();
                    }
                }

                const asset = category === EXTERNAL || category === INTERNAL
                    ? "ETH"
                    : "ERC20";

                acc = await acc;
                if (!acc[asset]) acc[asset] = [transferData];
                else acc[asset].push(transferData);

                return {
                    ...acc,
                };
            } else {
                throw new Error("Unsupported category");
            }
        }, Promise.resolve({})
    );
}

function generatePayload(params) {
    if (params.uuid === DEFAULT_UUID) delete params.uuid;

    return {
        method: "alchemy_getAssetTransfers",
        id: 1,
        jsonrpc: "2.0",
        params: [{
            excludeZeroValue: false,
            category: [
                "external",
                "internal",
                "erc20"
            ],
            ...params,
        }],
    }
}

async function fetchTransfers(params) {
    return new Promise((resolve, reject) => {
        web3.currentProvider.send(
            generatePayload(params), (
                err,
                res
            ) => {
                if (err) reject(err)
                resolve(res);
            });
    });
}

async function updateLatestFetchedBlockForAccounts(
    accounts,
    lastFetchedBlockNum
) {
    const conn = await pool.getConnection();
    await conn.batch(
        "REPLACE INTO UserSyncedBlock (" +
        "address," +
        " blockNum" +
        ")" +
        " VALUES (?, ?)",
        accounts.map(account => [account, lastFetchedBlockNum])
    );
    await conn.commit();
    await conn.release();
    await conn.end();
}

async function getLastFetchedBlockForAccounts(accounts) {
    const conn = await pool.getConnection();
    const accountsAndLastFetchedBlocks = await accounts.reduce(
        async (
            acc,
            account
        ) => {
            const blockNum = await conn.query(
                "SELECT blockNum from UserSyncedBlock where 'address' = " +
                account.toLowerCase()
            );
            acc = await acc;
            return {
                ...acc,
                [account]: toHex(
                    blockNum.length ? blockNum : 0
                ).toString(),
            }
        }, Promise.resolve({})
    );
    await conn.release();
    return accountsAndLastFetchedBlocks;
}

function aggregateData(data) {
    return ASSET_MOVEMENT.reduce(
        (
            acc,
            movement
        ) => {
            return [
                ...acc,
                ...ASSET_TYPES.reduce(
                    (
                        acc,
                        type
                    ) => {
                        return [
                            ...acc,
                            ...(data[movement][type].map(
                                ({
                                     txHash,
                                     from,
                                     to,
                                     name,
                                     value,
                                     address,
                                     decimal,
                                     blockNum,
                                     timestamp,
                                     gasCostInWei,
                                 }) => [
                                    txHash,
                                    from,
                                    to,
                                    name,
                                    value,
                                    address,
                                    decimal,
                                    blockNum,
                                    timestamp,
                                    gasCostInWei ? gasCostInWei : "none",
                                ]
                            ))
                        ]
                    }, []
                )
            ];
        }, []
    );
}

async function populateDatabase(data) {
    const conn = await pool.getConnection();
    await Object.keys(data).reduce(
        async (
            acc,
            account,
        ) => {
            await conn.batch(
                "INSERT INTO Transfer (" +
                "txHash," +
                " fromAddress," +
                " toAddress," +
                " assetName," +
                " value," +
                " contractAddress," +
                " assetDecimal," +
                " blockNum," +
                " timestamp," +
                " gasCostInWei" +
                ") " +
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                aggregateData(data[account])
            );
            await conn.commit();
            await conn.release();
            await acc;
        }, Promise.resolve()
    );
}