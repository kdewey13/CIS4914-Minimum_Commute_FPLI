import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="FPLI_Minimum_Commutes",
    version="10",
    author="Kelsey Dewey",
    author_email="fplimincomm@gmail.com",
    description="FPLI Inter-District Minimum Commute Estimator",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kdewey13/CIS4914-Minimum_Commute_FPLI",
    packages=setuptools.find_packages(),
    install_requires=[
        'geopy',
        'pandas',
        'requests',
        'pytz',
        'tzlocal',
        'openpyxl',
        'xlsxwriter'],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
)