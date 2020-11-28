import minimum_commute_calculator
import config  # local API key


def main():
    minimum_commute_calculator.commute_calculator(api_key=config.distance_key, distance_pairs_determination=False,
                                                  distance_pairs_csv='distance_pairs_small.csv', make_api_calls=True)


if __name__ == '__main__':
    main()