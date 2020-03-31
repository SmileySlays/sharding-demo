import json
import os
import collections
from shutil import copyfile
from typing import List, Dict

filename = "chapter2.txt"


def load_data_from_file(path=None) -> str:
    with open(path if path else filename, 'r') as f:
        data = f.read()
    return data


class ShardHandler(object):
    """
    Take any text file and shard it into X number of files with
    Y number of replications.
    """

    def __init__(self):
        self.mapping = self.load_map()
        self.last_char_position = 0

    mapfile = "mapping.json"

    def write_map(self) -> None:
        """Write the current 'database' mapping to file."""
        with open(self.mapfile, 'w') as m:
            json.dump(self.mapping, m, indent=2)

    def load_map(self) -> Dict:
        """Load the 'database' mapping from file."""
        if not os.path.exists(self.mapfile):
            return dict()
        with open(self.mapfile, 'r') as m:
            return json.load(m)

    def _reset_char_position(self):
        self.last_char_position = 0

    def get_shard_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' not in key])

    def get_replication_ids(self):
        return sorted([key for key in self.mapping.keys() if '-' in key])

    def build_shards(self, count: int, data: str = None) -> [str, None]:
        """Initialize our miniature databases from a clean mapfile. Cannot
        be called if there is an existing mapping -- must use add_shard() or
        remove_shard()."""
        if self.mapping != {}:
            return "Cannot build shard setup -- sharding already exists."

        spliced_data = self._generate_sharded_data(count, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

    def _write_shard_mapping(self, num: str, data: str, replication=False):
        """Write the requested data to the mapfile. The optional `replication`
        flag allows overriding the start and end information with the shard
        being replicated."""
        if replication:
            parent_shard = self.mapping.get(num[:num.index('-')])
            self.mapping.update(
                {
                    num: {
                        'start': parent_shard['start'],
                        'end': parent_shard['end']
                    }
                }
            )
        else:
            if int(num) == 0:
                # We reset it here in case we perform multiple write operations
                # within the same instantiation of the class. The char position
                # is used to power the index creation.
                self._reset_char_position()

            self.mapping.update(
                {
                    str(num): {
                        'start': (
                            self.last_char_position if
                            self.last_char_position == 0 else
                            self.last_char_position + 1
                        ),
                        'end': self.last_char_position + len(data)
                    }
                }
            )

            self.last_char_position += len(data)

    def _write_shard(self, num: int, data: str) -> None:
        """Write an individual database shard to disk and add it to the
        mapping."""
        if not os.path.exists("data"):
            os.mkdir("data")
        with open(f"data/{num}.txt", 'w') as s:
            s.write(data)
        self._write_shard_mapping(str(num), data)

    def _generate_sharded_data(self, count: int, data: str) -> List[str]:
        """Split the data into as many pieces as needed."""
        splicenum, rem = divmod(len(data), count)

        result = [data[splicenum * z:splicenum * (z + 1)] for z in range(count)]
        # take care of any odd characters
        if rem > 0:
            result[-1] += data[-rem:]

        return result

    def load_data_from_shards(self) -> str:
        """Grab all the shards, pull all the data, and then concatenate it."""
        result = list()

        for db in self.get_shard_ids():
            with open(f'data/{db}.txt', 'r') as f:
                result.append(f.read())
        return ''.join(result)

    def add_shard(self) -> None:
        """Add a new shard to the existing pool and rebalance the data."""
        self.mapping = self.load_map()
        data = self.load_data_from_shards()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        # why 2? Because we have to compensate for zero indexing
        new_shard_num = max(keys) + 2

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def remove_shard(self) -> None:
        """Loads the data from all shards, removes the extra 'database' file,
        and writes the new number of shards to disk.
        """
        data = self.load_data_from_shards()
        self.mapping = self.load_map()
        max_key = max([int( key_.split('-')[0] ) for key_ in self.get_all_shard_data().keys()] )
        
        end_shard = self.get_shard_data( str(max_key) )
        self.mapping.pop(str(max_key) )


        
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        new_shard_num = max(keys) + 1

        spliced_data = self._generate_sharded_data(new_shard_num, data)

        dir_ = os.getcwd()
        os.remove(dir_ + "/data/" + str(max_key) + ".txt")

        for num, d in enumerate(spliced_data):
            self._write_shard(num, d)

        self.write_map()

        self.sync_replication()

    def add_replication(self) -> None:
        """Add a level of replication so that each shard has a backup. Label
        them with the following format:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        2-1.txt (shard 2, replication 1)
        ...etc.

        By default, there is no replication -- add_replication should be able
        to detect how many levels there are and appropriately add the next
        level.
        """
        self.mapping = self.load_map()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        shard_num = max(keys)
        print(keys)
        key_rep = collections.defaultdict(int)
        for keyi in keys:
            key_rep[keyi] = max(key_rep[keyi],0)
        
        keys = [(int(i),int(j)) for i,j in [i.split('-') for i in self.get_replication_ids()]]
        for i,j in keys:
            key_rep[i] = max(key_rep[i],j)
        data = self.load_data_from_shards()
        for key in key_rep:
            num = str(key) + "-" + str(key_rep[key] + 1)
            self._write_shard_mapping(num, data, True)
        print(self.mapping)

        # spliced_data = self._generate_sharded_data(shard_num, data)
        dir_ = os.getcwd()
        for key in self.mapping.keys():
            print(key)
            if "-" in str(key):
                with open(dir_ + '/data/' + key + ".txt", 'w'):
                    source = dir_ + '/data/' + key[:key.index("-")] + ".txt"
                    destination = dir_ + '/data/' + key + ".txt"
                    copyfile(source, destination)

        self.write_map()

        self.sync_replication()

    def remove_replication(self) -> None:
        """Remove the highest replication level.

        If there are only primary files left, remove_replication should raise
        an exception stating that there is nothing left to remove.

        For example:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        1-2.txt (shard 1, replication 2)
        2.txt (shard 2, primary)
        etc...

        to:

        1.txt (shard 1, primary)
        1-1.txt (shard 1, replication 1)
        2.txt (shard 2, primary)
        etc...
        """
        self.mapping = self.load_map()
        keys = [int(z) for z in self.get_shard_ids()]
        keys.sort()
        shard_num = max(keys)
        key_rep = collections.defaultdict(int)
        for keyi in keys:
            key_rep[keyi] = max(key_rep[keyi],0)
        
        keys = [(int(i),int(j)) for i,j in [i.split('-') for i in self.get_replication_ids()]]
        for i,j in keys:
            key_rep[i] = max(key_rep[i],j)
        
        dir_ = os.getcwd()
        for key in key_rep:
            if key_rep[key]:
                os.remove(dir_ + "/data/" + str(key) + "-" + str(key_rep[key]) + ".txt")
                shard = str(key) + "-" + str(key_rep[key])
                self.mapping.pop(shard)
            
            else:
                raise(NameError("There is nothing left to remove!"))

        self.write_map()

        self.sync_replication()

    def sync_replication(self) -> None:
        """Verify that all replications are equal to their primaries and that
        any missing primaries are appropriately recreated from their
        replications."""

        self.mapping = self.load_map()
        dir_ = os.getcwd()
        files = {}
        max_rep = 0
        for key in self.mapping.keys():
            primary = key
            secondary = 0
            if '-' in key:
                primary = key.split('-')[0]
                secondary = int(key.split('-')[1])
                max_rep = max(max_rep, secondary)
            if primary not in files:
                files[primary] = {'all_keys':set(), 'filename': '', 'max_rep':secondary} 
            files[primary]['all_keys'].add(key)
            files[primary]['max_rep'] = max(files[primary]['max_rep'], secondary)
            if os.path.exists(dir_ + '/data/' + key + ".txt"):
                files[primary]['filename'] = dir_ + '/data/' + key + ".txt"
                files[primary]['all_keys'].remove(key)

        for primary in files:
            for secondary in range(files[primary]['max_rep']+1, max_rep+1):
                num = '-'.join([str(primary), str(secondary)])
                files[primary]['all_keys'].add(num)
                self._write_shard_mapping(num, '' , replication=True)

        for primary in files:
            source = files[primary]['filename']
            for key in files[primary]['all_keys']:
                destination = dir_ + '/data/' + key + ".txt"
                copyfile(source, destination)
        
        

        self.write_map()



    def get_shard_data(self, shardnum=None) -> [str, Dict]:
        """Return information about a shard from the mapfile."""
        if not shardnum:
            return self.get_all_shard_data()
        data = self.mapping.get(shardnum)
        if not data:
            return f"Invalid shard ID. Valid shard IDs: {self.get_shard_ids()}"
        return f"Shard {shardnum}: {data}"

    def get_all_shard_data(self) -> Dict:
        """A helper function to view the mapping data."""
        return self.mapping


s = ShardHandler()

s.build_shards(5, load_data_from_file())

print(s.mapping.keys())
# s.add_shard()
# s.remove_shard()
# s.add_replication()
# s.remove_replication()
s.sync_replication()
# print(s.mapping)

print(s.mapping.keys())
