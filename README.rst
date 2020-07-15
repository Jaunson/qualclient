==========
QualClient
==========


.. image:: https://img.shields.io/pypi/v/qualclient.svg
        :target: https://pypi.python.org/pypi/qualclient

.. image:: https://img.shields.io/travis/Jaunson/qualclient.svg
        :target: https://travis-ci.com/Jaunson/qualclient

.. image:: https://readthedocs.org/projects/qualclient/badge/?version=latest
        :target: https://qualclient.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status




Pythonic API Client that exports Qualtrics Data straight to Pandas DataFrame


* Free software: MIT license
* Documentation: https://qualclient.readthedocs.io.


Features
--------

    QualClient is a python wrapper the provides convenient access to data
    exports directly from Qualtrics into Pandas for further manipulation.
    
    The client in intiated with an API Token, and API URL
    It provides 3 Primary functions-
    
    QualClient.pull_survey_meta():
        Pulls down a complete list of your surveys and addtional parameters
        such as isActive, Creation Date, Mod Date, Name, and IDs
        
    QualClient.pull_definition(survey_id):
        survey_id : str
        Takes the supplied survey_id and returns a df with the
        survey's defintion info, which identifies things like the 
        questions asked, question text, question order, and IDs
        
    QualClient.pull_results(survey_id):
        survey_id : str
        Take the supplied survey_id and returns a df of all of the responses
        to the survey, with both the raw text and encoding of the response.
        This functionalty actually downloads and unzips files from Qualtrics, so be
        aware that it might take a moment to return the finalized data.
        DF takes the shape of a long table with one response per row.


Credits
-------

This package was created with Cookiecutter_ and the `audreyr/cookiecutter-pypackage`_ project template.

.. _Cookiecutter: https://github.com/audreyr/cookiecutter
.. _`audreyr/cookiecutter-pypackage`: https://github.com/audreyr/cookiecutter-pypackage
