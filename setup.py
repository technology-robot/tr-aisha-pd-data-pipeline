# Copyright (c) Carted.

# All rights reserved.

# This source code is licensed under the license found in the
# LICENSE file in the root directory of this source tree.

from setuptools import setup, find_packages


NAME = "tr-aisha-for-product-recommendation-data-pipeline"
VERSION = "0.0.1"

with open("requirements.txt") as f_p:
    REQUIRED_PACKAGES = f_p.readlines()

setup(
    name=NAME,
    version=VERSION,
    install_requires=REQUIRED_PACKAGES,
    packages=find_packages(),
    include_package_data=False,
)
