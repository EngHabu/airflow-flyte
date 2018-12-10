#!/usr/bin/env python

from compiler import compiler

from lyftdata.airflow.dags.experimentation import experimentation_boostrap_exposures


def main():
    co = compiler.FlyteCompiler()
    co.compile(experimentation_boostrap_exposures.dag, '')


if __name__ == "__main__":
    main()
