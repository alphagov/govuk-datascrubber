import setuptools
import os

setuptools.setup(
    name="datascrubber",
    version=os.environ.get('VERSION'),
    author="GOV.UK Reliability Engineering",
    author_email="reliability-engineering@digital.cabinet-office.gov.uk",
    description="Scrubs sensitive data from databases",
    url="https://github.com/alphagov/govuk-datascrubber",
    packages=setuptools.find_packages(),
    package_data={'datascrubber': ['sql/*.sql']},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
