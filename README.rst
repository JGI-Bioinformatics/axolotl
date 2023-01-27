=======
Axolotl
=======


A Python Genomics Library based on pySpark


* Free software: BSD license
* Documentation: https://


Features
--------

Install
--------
  1. Install Java UDFs
  ```
  cd sparcudfs
  mvn clean package
  cd ..
  ```
  If successful, you will see a jar file: ./target/sparcudfs-X.X.jar
  2. Package the tar.gz for pip
  ```
  python setup.py sdist
  ```
  3. On the target machine
  ```
  pip install dist/axolotl-0.1.1.tar.gz
  ```


* TODO

Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
