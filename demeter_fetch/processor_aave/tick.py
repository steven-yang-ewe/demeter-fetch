import pandas as pd
import demeter_fetch.processor_aave.aave_utils as aave_utils


def preprocess_one(df: pd.DataFrame):
    df["tx_type"] = df.apply(lambda x: aave_utils.get_tx_type(x.topics), axis=1)
    df[
        [
            "reserve",
            "owner",
            "amount",
            "interest_rate_mode",
            "borrow_rate",
            "collateral_asset",
            "debt_asset",
            "liquidated_collateral_amount",
            "liquidator",
        ]
    ] = df.apply(
        lambda x: aave_utils.handle_event(x.tx_type, x.topics, x.DATA),
        axis=1,
        result_type="expand",
    )
    df["tx_type"] = df.apply(lambda x: x.tx_type.name, axis=1)
    columns = [
        'block_number', 'transaction_hash', 'block_timestamp', 'transaction_index', 'log_index', 'token', 'tx_type',
        'reserve', 'owner', 'amount', 'interest_rate_mode', 'borrow_rate', 'collateral_asset', 'debt_asset',
        'liquidated_collateral_amount', 'liquidator'
    ]
    df = df[columns]
    return df
