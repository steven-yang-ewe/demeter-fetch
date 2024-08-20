import time
from datetime import datetime, timedelta

from demeter_fetch import ToType, ToConfig, ToFileType, ChainType, DataSource, FromConfig, DappType, RpcConfig, \
    UniswapConfig
from demeter_fetch.core import download_by_config
from demeter_fetch.common import Config


def main():

    # pool_address = "0xc473e2aEE3441BF9240Be85eb122aBB059A3B57c"  # arbitrum weth/usdc 0.3
    # pool_address = "0xC6962004f452bE9203591991D15f6b388e09E8D0"  # arbitrum weth/usdc 0.05 Jun-08-2023 09:28:24 PM +UTC
    # pool_address = "0x2f5e87C9312fa29aed5c179E456625D79015299c" # arbitrum wbtc/weth 0.05

    # pool_address = "0x88e6a0c2ddd26feeb64f039a2c41296fcb3f5640"  # ethereum usdc/weth
    # pool_address = "0xCBCdF9626bC03E24f779434178A73a0B4bad62eD"  # ethereum wbtc/eth 0.3
    pool_address = "0x4585FE77225b41b697C938B018E2Ac67Ac5a20c0" # ethereum wbtc/weth 0.05 May-05-2021 08:23:50 PM UTC

    etherscan_keys = {
        ChainType.arbitrum: "3JWJSC8YTAD7AYU276BU44R7QTV16S4UM2",
        ChainType.ethereum: "HVXV2T3D24MZYV3YMT9TGCZHG86YH1KU25"
    }
    # Steven's arbiscan: J9GASPSIWKS5RNRAKMI8R2T3YRFKBQCCVQ
    # Kamel's arbiscan: 3JWJSC8YTAD7AYU276BU44R7QTV16S4UM2
    batch_sizes = {
        ChainType.arbitrum: 5000,
        ChainType.ethereum: 1000
    }
    # "https://mainnet.infura.io/v3/48f642e58a414fa0bbd803c97da23cc1",
    hosts = ["https://go.getblock.io/f49d7417ba474479b4641a2a6bbde2e1",
        "https://go.getblock.io/7584161274204268a65fc71d9d142043",
        "https://go.getblock.io/ebbd88238ab7462cb8c42af67ec66f32",
        "https://go.getblock.io/e507c19016e44868b6c5117a00bd350e",
             ]

    # hosts = ["https://arb-mainnet.g.alchemy.com/v2/PUECRZoAOlLd9RzrTgW32Yv9gmKsIBV6",
    #          "https://arb-mainnet.g.alchemy.com/v2/53Y2hvshgWivNTRugCBELe6WGtY6ySct",
    #          "https://arb-mainnet.g.alchemy.com/v2/dlY8gvYM4ZJYvowh-JrCGPDEd5FLgNR4",
    #          "https://arb-mainnet.g.alchemy.com/v2/KLmJncAMynx8Eh5-vueyu8oEOEw1o60n"]
    # hosts = ["https://eth-mainnet.g.alchemy.com/v2/KLmJncAMynx8Eh5-vueyu8oEOEw1o60n",
    #          "https://eth-mainnet.g.alchemy.com/v2/dlY8gvYM4ZJYvowh-JrCGPDEd5FLgNR4",
    #          "https://eth-mainnet.g.alchemy.com/v2/PUECRZoAOlLd9RzrTgW32Yv9gmKsIBV6",
    #          "https://eth-mainnet.g.alchemy.com/v2/53Y2hvshgWivNTRugCBELe6WGtY6ySct"
    #          ]
    #
    chain = ChainType.ethereum
    # chain = ChainType.arbitrum
    etherscan_api_key = etherscan_keys[chain]
    batch_size = batch_sizes[chain]

    to_type = ToType.minute
    save_path = "./sample"
    multi_process = False
    skip_existed = True
    keep_raw = False
    to_file_type = ToFileType.csv
    data_source = DataSource.rpc
    dapp_type = DappType.uniswap
    http_proxy = None

    file_name = pool_address + "-startdate"
    f = open(file_name, "r")
    start_date = f.read()
    f.close()

    print("start_date: " + start_date)
    # start = datetime(2024, 1, 1)
    start = datetime.strptime(start_date.strip(), "%Y-%m-%d")

    for host in hosts:

        today = datetime.now()
        # dateCount = (8 if "alchemy" in host else 3) - 1
        date_count = 10

        begin_date = start

        end_date = start + timedelta(days=date_count)

        if begin_date > today:
            print("begin_date (" + begin_date.strftime("%Y-%m-%d") + ") is in the future")
            break

        if end_date > today:
            end_date = today

        start_time = begin_date
        end_time = end_date

        print("running for host: " + host + ", from: " + start_time.strftime("%Y-%m-%d") + " to: " + end_time.strftime(
            "%Y-%m-%d"))
        success = False
        for x in range(50):
            to_config = ToConfig(to_type, save_path, multi_process, skip_existed, keep_raw, to_file_type)
            from_config = FromConfig(
                chain=chain, data_source=data_source, dapp_type=dapp_type, http_proxy=http_proxy, start=start_time,
                end=end_time
            )

            # pool_address = conf_file["from"]["uniswap"]["pool_address"].lower()
            ignore_position_id = True
            from_config.uniswap_config = UniswapConfig(pool_address, ignore_position_id)
            # if "token0" in conf_file["from"]["uniswap"]:
            #     token0_name = get_item_with_default_4(conf_file, "from", "uniswap", "token0", "name", "")
            #     token0_decimal = get_item_with_default_4(conf_file, "from", "uniswap", "token0", "decimal", 0)
            #     from_config.uniswap_config.token0 = TokenConfig(token0_name, token0_decimal)
            # if "token1" in conf_file["from"]["uniswap"]:
            #     token1_name = get_item_with_default_4(conf_file, "from", "uniswap", "token1", "name", "")
            #     token1_decimal = get_item_with_default_4(conf_file, "from", "uniswap", "token1", "decimal", 0)
            #     from_config.uniswap_config.token1 = TokenConfig(token1_name, token1_decimal)
            from_config.uniswap_config.is_token0_base = None

            auth_string = None
            keep_tmp_files = False

            end_point = host
            # batch_size = get_item_with_default_3(conf_file, "from", "rpc", "batch_size", 500)
            force_no_proxy = False
            from_config.rpc = RpcConfig(
                end_point=end_point,
                batch_size=batch_size,
                auth_string=auth_string,
                keep_tmp_files=keep_tmp_files,
                etherscan_api_key=etherscan_api_key,
                force_no_proxy=force_no_proxy,
            )

            config = Config(from_config, to_config)

            try:
                result = download_by_config(config)
                print("result: " + str(result))
                success = True
                break
            except TypeError as e:
                print("TypeError (" + str(x) + "): " + str(e))
                time.sleep(5)
            except RuntimeError as e:
                print("RuntimeError (" + str(x) + "): " + str(e))
                time.sleep(5)
        pass

        if success:
            start = end_date + timedelta(days=1)
            with open(file_name, "w") as f:
                f.write(start.strftime("%Y-%m-%d"))
        else:
            exit(1)

    pass


if __name__ == "__main__":
    main()
