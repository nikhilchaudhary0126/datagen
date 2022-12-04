from datagen.attributes import Attribute


class Distribution(Attribute):
    def __init__(self, name: str):
        super().__init__(name)

    def add_probability_distribution(self, events, probability):
        return ProbabilityDistribution(self.name, events, probability)

    def add_normal_distribution(self, mean, standard_deviation):
        return NormalDistribution(self.name, mean, standard_deviation)

    def add_logistic_distribution(self, mean, standard_deviation):
        return LogisticDistribution(self.name, mean, standard_deviation)

    def add_exponential_distribution(self, scale):
        return ExponentialDistribution(self.name, scale)

    def add_binomial_distribution(self, total_trials, probability):
        return BinomialDistribution(self.name, total_trials, probability)

    def add_poisson_distribution(self, event_rate):
        return PoissonDistribution(self.name, event_rate)


class ProbabilityDistribution(Distribution):
    def __init__(self, name: str, distribution_path: str, events_filename: str, weights_filename: str):
        super().__init__(name)
        self.path = distribution_path
        self.events_file = events_filename
        self.weights_file = weights_filename


class MixedProbabilityDistribution:
    def __init__(self, names: list[str], distribution_path: str, events_filenames: list[str], weights_filename: str):
        self.names = names
        self.path = distribution_path
        self.events_files = events_filenames
        self.weights_file = weights_filename


class NormalDistribution(Distribution):
    def __init__(self, name: str, mean: float | int, standard_deviation: float | int):
        super().__init__(name)
        self.mean = mean
        self.standard_deviation = standard_deviation


class LogisticDistribution(Distribution):
    def __init__(self, name: str, mean: float | int, standard_deviation: float | int):
        super().__init__(name)
        self.mean = mean
        self.standard_deviation = standard_deviation


class ExponentialDistribution(Distribution):
    def __init__(self, name: str, scale: float | int):
        super().__init__(name)
        self.scale = scale


class BinomialDistribution(Distribution):
    def __init__(self, name: str, total_trials: float | int, probability: float | int):
        super().__init__(name)
        self.total_trials = total_trials
        self.probability = probability


class PoissonDistribution(Distribution):
    def __init__(self, name: str, event_rate: float | int):
        super().__init__(name)
        self.event_rate = event_rate
