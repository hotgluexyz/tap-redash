from setuptools import setup

setup(
    name='tap-redash',
    version='0.1.0',
    description='Singer tap for extracting data from the Redash API',
    url='https://github.com/hotgluexyz/tap-redash',
    author='Brendan Hogan',
    classifiers=['Programming Language :: Python :: 3 :: Only'],
    py_modules=['tap_redash'],
    long_description=open('README.md').read(),
    install_requires=[
        'singer-python>=2.1.4',
        'requests>=2.20.0',
    ],
    entry_points='''
        [console_scripts]
        tap-redash=tap_redash:main
    ''',
    python_requires='>=3.7',
)