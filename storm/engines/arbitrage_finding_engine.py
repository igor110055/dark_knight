from multiprocessing import Process

from ..services.arbitrage_service import get_arbitrage_opportunity
from ..utils import get_logger, load_yaml

logger = get_logger(__file__)


EXPECTED_RETURN = 0.0025


def main():
    logger.info("start arbitrage")

    trading_groups = load_yaml("trading_groups.yml")

    # a process for each trading group
    for trading_group in trading_groups:
        Process(
            target=get_arbitrage_opportunity, args=(trading_group, EXPECTED_RETURN)
        ).start()


if __name__ == "__main__":
    # TODO: add last updated timestamp to indicate if every symbol in the group is fresh

    main()
