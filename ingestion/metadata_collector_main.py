import sys
from metadata_collector import metadata_collector_execute

if __name__ == "__main__":
    pass

    metadata_collector_execute(system_id=sys.argv[1],
    # # metadata_collector_execute(system_id='hive-test2',
                    sink='metadata-rest')

    # metadata_collector_execute(system_id='b86ae1be-7370-4d36-bdff-c47e1429ee34',
    #                 sink='metadata-rest')

    # metadata_collector_execute(system_name=sys.argv[1], sink="file")
    # metadata_collector_execute(system_id="109ae637-9e13-44f9-9686-a79cf1e12499", sink="file")
    # metadata_collector_execute(system_id="phy-test", sink="file")

