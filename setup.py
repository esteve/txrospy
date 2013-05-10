setupdict= {
    'name': 'txROSpy',
    'version': '0.1',
    'author': 'Esteve Fernandez',
    'author_email': 'esteve@apache.org',
    'url': 'http://github.com/esteve/txrospy',
    'description': 'Twisted port of the ROS protocols',
    'long_description': '''txROSpy provides implementations of the ROS protocols (publisher, subscriber, service and client) suitable for their use in Twisted asynchronous applications.'''
    }

try:
    from setuptools import setup, find_packages
except ImportError:
    from distutils.core import setup
    setupdict['py_modules'] = ['txrospy']
else:
    setupdict['py_modules'] = ['txrospy']
    setupdict['install_requires'] = ['Twisted']

setup(**setupdict)
