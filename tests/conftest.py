import warnings

# Suppress noisy pydantic / typing deprecation warnings during tests
warnings.filterwarnings("ignore", category=DeprecationWarning, module="pydantic.*")
warnings.filterwarnings("ignore", category=FutureWarning, module="pydantic.*")
# Also suppress pandas futurewarnings that are benign for our tests
warnings.filterwarnings("ignore", category=FutureWarning, module="pandas.*")
