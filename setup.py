from setuptools import setup

setup(
    name='dagonstar',
    version='0.1',
    packages=['dagon', 'dagon.api', 'dagon.cloud', 'dagon.communication', 'dagon.visualization',
              'dagon.dockercontainer'],
    url='http://github.com/dagonstar/',
    license='Apache 2.0',
    author='Raffaele Montella',
    author_email='raffaele.montella@uniparthenope.it',
    description='DAGon* is a simple Python based workflow engine able to run job on everything from the local machine to distributed virtual HPC clusters hosted in private and public clouds.'
)
