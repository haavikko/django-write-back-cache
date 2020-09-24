import os
from setuptools import setup, find_packages
 
setup(name='django-write-back-cache',
    version="0.1",
    description='Write back cache for Django models',
    author='Matti Haavikko',
    author_email='haavikko@gmail.com',
    url='http://github.com/haavikko/django-write-back-cache',
    packages=find_packages(),
    classifiers=[
        "Development Status :: 3 - Alpha",
        "Framework :: Django",
        "Intended Audience :: Developers",
        "Operating System :: OS Independent",
        "Topic :: Software Development"
    ],
)
