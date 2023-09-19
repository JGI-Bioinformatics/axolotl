import pyspark.sql.functions as F
import pyspark.sql.types as T

from axolotl.app.base import AxlApp
from axolotl.io.genbank import gbkIO
from axolotl.data.sequence import NuclSeqDF
from axolotl.data.annotation import cdsDF

from abc import abstractmethod
from typing import Dict
from os import path
import pickle


class ExampleApp(AxlApp):

    @classmethod
    @abstractmethod
    def _dataDesc(cls) -> Dict:
        # describe the app's "database" structure, in this case
        # it will have two tables: "contigs" (a NuclSeqDF) and
        # "cds" (a cdsDF). A relationship between "cds" and "contigs"
        # is established via cds.source_path == contigs.file_path
        # and cds.seq_id == contigs.seq_id (see NuclSeqDF and cdsDF
        # schema for details)

        return {
            "contigs": NuclSeqDF,
            "cds": cdsDF
        }

    @abstractmethod
    def _creationFunc(self, genbanks_path: str):
        # we override the *args and **kwargs with our own class-specific
        # parameters. In this case an exampleApp will be created by
        # supplying the path (in glob pattern) of genbank file of the
        # input genomes. Then, in this _creationFunc method, we will use
        # gbkSeqIO and gbkFeatIO's loadSmallFiles() functions to parse
        # and store the "contigs" and "cds" table of the app.

        contigs_df = gbkIO.loadSmallFiles(genbanks_path, df_type="sequence")
        self._setData("contigs", contigs_df) # associate AxlDF with the table
        self._saveData("contigs") # execute and store the actual spark query

        feature_df = gbkIO.loadSmallFiles(genbanks_path, df_type="annotation")
        cds_df = cdsDF.fromRawFeatDF(feature_df, contigs_df)
        self._setData("cds", cds_df)
        self._saveData("cds")

        # to demonstrate the additional flexibility of AxlApp, we create a
        # pickle file that we can use to load an extra data within the ExampleApp
        # object.
        self._dummy_metadata = "just_a_string"
        with open(path.join(self._folder_path, "dummy.pkl"), "wb") as file:
            pickle.dump(self._dummy_metadata, file)
        
    @abstractmethod
    def _loadExtraData(self):
        # here, we can load back the _dummy_metadata variable from the pickled file
        with open(path.join(self._folder_path, "dummy.pkl"), "rb") as file:
            self._dummy_metadata = pickle.load(file)

    def getCDScount(self):
        # a demonstration of app-specific functions. With this method, users can
        # query the previosly-created database and get a tally of CDS counts per
        # genomes (i.e., gbk path) as a pySpark DataFrame.

        return self._getData("cds").df.groupBy("source_path").count()