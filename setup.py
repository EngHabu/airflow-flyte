from __future__ import absolute_import

from setuptools import setup, find_packages

import compiler

setup(
    name='airflow-flyte-compiler',
    version=compiler.__version__,
    maintainer='EngHabu',
    maintainer_email='haytham@afutuh.com',
    packages=find_packages(),
    url='https://github.com/lyft/modelbuilderapi',
    description='Lyft Modelbuilder API',
    long_description=open('README.md').read(),
    entry_points={
        'console_scripts': [
            # 'mbadmin=lyft_modelbuilder.tools.cli_modules:admin_groups',
            # 'mbcli=lyft_modelbuilder.tools.run_workflow:cli',
            # 'mbsystem=lyft_modelbuilder.tools.mbsystem_impl:batchrunner',
            # 'pyflyte-execute=flytekit.bin.entrypoint:execute_task_cmd',
            # 'pyflyte=flytekit.clis.sdk_in_container.pyflyte:main',
            # 'flyte-cli=flytekit.clis.flyte_cli.main:_flyte_cli'
        ]
    },
    install_requires=[
        "modelbuilderapi",
        "apache-airflow",
    ],
    scripts=[
    ],
    python_requires=">=2.7"
)
