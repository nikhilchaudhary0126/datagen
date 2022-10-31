import setuptools

setuptools.setup(name='datagen',
                 version='0.0.1',
                 description='Python package to generate data',
                 url='#',
                 author='Nikhil Chaudhary',
                 install_requires=['pandas>=1.4.4'],
                 author_email='',
                 packages=setuptools.find_packages(),
                 package_dir={'datagen': 'datagen'},
                 zip_safe=False,
                 python_requires=">=3.10")
