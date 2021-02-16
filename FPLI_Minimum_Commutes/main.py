import minimum_commute_calculator
import config
import config  # local API key


def main():
    minimum_commute_calculator.commute_calculator(distance_pairs_determination=False,
                                                  api_key=config.distance_key,
                                                  make_api_calls=True)


if __name__ == '__main__':
    main()