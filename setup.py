from setuptools import setup, find_packages

readme = open('README.rst').read()


install_requires=[
    'future',
    'gitpython',
    'jsonschema',
    'passlib',
    'psycopg2-binary',
    'python-dateutil',
    'pyyaml>=5.1'
]


tests_require = [
    'coverage>=4.0',
    'pytest',
    'pytest-cov',
    'tox'
]


extras_require = {
    'docs': [
        'Sphinx',
        'sphinx-rtd-theme'
    ],
    'tests': tests_require,
}


setup(
    name='benchmark-templates',
    version='0.2.0',
    description='Workflow Templates for Reproducible Open Benchmarks',
    long_description=readme,
    long_description_content_type='text/x-rst',
    keywords='reproducibility benchmarks data analysis',
    url='https://github.com/scailfin/benchmark-templates',
    author='Heiko Mueller',
    author_email='heiko.muller@gmail.com',
    license='MIT',
    packages=find_packages(exclude=('tests',)),
    include_package_data=True,
    extras_require=extras_require,
    tests_require=tests_require,
    install_requires=install_requires,
    classifiers=[
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python'
    ]
)
