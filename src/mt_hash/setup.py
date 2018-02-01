#!/usr/bin/env python
# coding=utf-8


from distutils.core import setup, Extension

module = Extension('mt_hash', sources=['mt_hash.cpp'])
setup(name='mt_hash', version='1.0', ext_modules = [module])