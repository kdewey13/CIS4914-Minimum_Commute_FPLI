import minimum_commute_calculator
import config  # local API key


def main():
    minimum_commute_calculator.calculator(api_key=config.distance_key, distance_pairs_determination=False)


if __name__ == '__main__':
    main()