import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="FPLI_Minimum_Commutes", # Replace with your own username
    version="0.0.1",
    author="Kelsey Dewey",
    author_email="author@example.com",
    description="fplimincomm@gmail.com",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/kdewey13/CIS4914-Minimum_Commute_FPLI",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=2.7',
)