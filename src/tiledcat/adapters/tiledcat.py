import asyncio
from dataclasses import dataclass
from typing import Optional


from tiled.iterviews import ItemsView, KeysView, ValuesView
from tiled.adapters.hdf5 import HDF5Adapter
from tiled.utils import DictView
from pyscicat.client import ScicatClient, from_token
from pyscicat.model import Dataset

from .scicat_client import AsyncScicatClient

# class ScicatClient2():
#     """ Wrap features into scicat client that will eventually be added to the pyscicat library """

#     def __init__(        
#         self,
#         scicat_client: ScicatClient,
#         ) -> None:
#         self.scicat_client = scicat_client

#     def get_dataset_full_facet(self, query):
#         endpoint = f'Datasets/fullfacet?fields={{"mode":{{}},"text":"test"}}&facets=["type","creationTime","creationLocation","ownerGroup","keywords"]'
#         return self.scicat_client._call_endpoint(
#             cmd="get",
#             endpoint=endpoint,
#             operation="get_dataset_full_facet",
#             allow_404=True,
#         )

#     def get_dataset_by_pid(self, key):
#         return self.scicat_client.get_dataset_by_pid(key)
    
#     def get_datasets(self, query):
#         return self.scicat_client.get_datasets(query)
    
#     def datasets_find(
#         self, skip: int = 0, limit: int = 25, query_fields: Optional[dict] = None
#         ) -> Optional[dict]:

#         return self.scicat_client.datasets_find(skip, limit, query_fields=query_fields)



# class ScicatDatasetNode(collections.abc.Mapping):
#     structure_family = "node"
#     specs = ["ScicatCatalog"]

#     def __init__(self, metadata = {}, scicat_client: ScicatClient2=None):
#         self.scicat_client = scicat_client
#         self.metadata = metadata
#         super().__init__()
    
#     def __getitem__(self, key):
#         # pass down parent's scicat_client
#         dataset = self.scicat_client.get_dataset_by_pid(f"als/{key}")
#         entry = self._build_node_from_scientific_metadata(dataset)  
#         return entry

#     def __iter__(self):
#         pass
    
#     def __len__(self):
#         return 1

        
#     def _items_slice(self, start, stop, direction):
#         return [{"foo": "bar"}]
    
#     def _keys_slice(self, start, stop, direction):
#         return ["foo"]


#     def _build_node_from_scientific_metadata(self, scientific_metadata):
#         if scientific_metadata.get('dataFormat') == 'DX':
#             h5_adatper = HDF5Adapter.from_file(scientific_metadata.get('dataLocation'))
        

#     def keys(self):
#         return KeysView(lambda: len(self), self._keys_slice)

#     def values(self):
#         return ValuesView(lambda: len(self), self._items_slice)

#     def items(self):
#         return ItemsView(lambda: len(self), self._items_slice)


# class ScicatNode(collections.abc.Mapping):
#     structure_family = "node"
#     specs = ["ScicatCatalog"]
#     session = None
#     def __init__(self, metadata = {}, scicat_client: ScicatClient2=None):
#         self.scicat_client = scicat_client
#         self.metadata = metadata
#         super().__init__()
    
#     def __getitem__(self, key):
#         # pass down parent's scicat_client
#         dataset = self.scicat_client.get_dataset_by_pid(f"als/{key}")
#         entry = self._build_node_from_scientific_metadata(dataset)  
#         return entry
    
#     def _build_node_from_scientific_metadata(self, scientific_metadata):
#         if scientific_metadata.get('dataFormat') == 'DX':
#             # h5_adatper = HDF5DatasetAdapter.from_file(scientific_metadata.get('dataLocation'))
#             h5_adatper = HDF5Adapter.from_file('/home/dylan/data/data/tomo/20230601_152444_C4_25ft_x00y02.h5')
#             # h5_adatper.metadata = scientific_metadata
#             return h5_adatper
#         return None

#     def __iter__(self):
#         pass
    
#     def __len__(self):
#         facets = self.scicat_client.get_dataset_full_facet({})
#         # facets produces information about this currently queries results, including
#         # number of derived and raw datasets that the query returned. This comes in list of 
#         # objects
#         count = 0
#         for type_facet in facets[0]['type']:
#             count += type_facet['count']
        
#         return count

        
#     def _items_slice(self, start, stop, direction):
#         datasets_list = self.scicat_client.datasets_find(start, stop - start)
#         datasets = dict([(dataset['pid'].split('/')[1], ScicatDatasetNode(dataset, self.scicat_client)) for dataset in datasets_list])
#         return datasets
    
#     def _keys_slice(self, start, stop, direction):
#         datasets_list = self.scicat_client.datasets_find(start, stop - start)
#         # build a dict with from this list with pid as the key.
#         # url encode the pid because it has / in it and this gets treated by tiled as a path separator
#         datasets = dict([(dataset['pid'].split('/')[1], ScicatDatasetNode(dataset, self.scicat_client)) for dataset in datasets_list])
#         return datasets

#     def keys(self):
#         return KeysView(lambda: len(self), self._keys_slice)

#     def values(self):
#         return ValuesView(lambda: len(self), self._items_slice)

#     def items(self):
#         return ItemsView(lambda: len(self), self._items_slice)
    
    # @classmethod
    # def from_uri(cls, scicat_uri: str, scicat_token: str):
    #     "Create a new instance from a SciCat URI"
    #     # dataset = scicat_client.get_dataset(uri)
    #     scicat_client = ScicatClient2(from_token(scicat_uri, scicat_token))
    #     return cls({}, scicat_client=scicat_client)
    





# def get_scicat_datasets(scicat_client: ScicatClient, start: int, stop: int):
#     datasets = scicat_client.get_datasets()
#     return datasets




class AsyncScicatAdapter():
    scicat_token = None
    metadata = {}
    structure_family = "container"
    specs = ["ScicatCatalog"]


    def __init__(self, scicat_client: AsyncScicatClient):
       self.scicat_client = scicat_client


    def with_session_state(self, session_state):
        self.scicat_token = session_state['scicat_token']
        return self
    
    
    def _build_node_from_scientific_metadata(self, scientific_metadata, path_parts: list[str]):
        if scientific_metadata.get('dataFormat') == 'DX':
            from functools import reduce
            import h5py
            
            h5_file = h5py.File('/home/dylan/data/data/tomo/20230601_152444_C4_25ft_x00y02.h5', 'r')
            # h5_adatper = HDF5Adapter.from_file(scientific_metadata.get('dataLocation'))
            
            if len(path_parts) > 1:
                path_parts = path_parts[1:]
                h5_file = reduce(lambda d, key: d[key], path_parts, h5_file)
            h5_adatper = HDF5Adapter(h5_file)
            h5_adatper.scientific_metadata = scientific_metadata
            return h5_adatper
        return None
   

    async def lookup_adapter(self, path_parts: list[str]):

        dataset = await self.scicat_client.datasets_get_one(f"als/{path_parts[0]}")
        entry = await asyncio.to_thread(self._build_node_from_scientific_metadata, dataset, path_parts)
        return entry
    

    async def keys_range(self, offset, limit):
        # Called for initial root node, go to SciCat and perform a search on all datasets
        datasets_list = self.scicat_client.datasets_find(offset, limit - offset)
        # build a dict with from this list with pid as the key.
        # url encode the pid because it has / in it and this gets treated by tiled as a path separator
        datasets = dict([(dataset['pid'].split('/')[1], None) for dataset in datasets_list])
        return datasets


    async def values_range(self, offset, limit):
        pass


    # @property
    # def scicat_client(self):
    #     return ScicatClient2(from_token(self.scicat_url, self.scicat_token))

    @classmethod
    def from_uri(cls, scicat_uri: str):
        "Create a new instance from a SciCat URI"
        return cls(AsyncScicatClient(scicat_uri, old_backend=True))
    
    lookup_node = lookup_adapter


class ScicatH5Adapter(HDF5Adapter):
    def __init__(self, node, *, specs=None, access_policy=None, scientific_metadata = None):
        super().__init__(node, specs=specs, access_policy=access_policy)
        self.scientific_metadata = scientific_metadata or {}

    @property
    def metadata(self):
        return DictView(self.scientific_metadata)
        