import sys
from metadata_collector import metadata_collector_execute

if __name__ == "__main__":
    pass

    # metadata_collector_execute(system_id=sys.argv[1],
    #                 sink='metadata-rest')

    metadata_collector_execute(system_id='3ac82118-5d3c-4a3f-b83b-70c883796d16',
                    sink='metadata-rest')

    # metadata_collector_execute(system_id='08c36eb4-9cca-4a08-b40e-b9d26d3cc174',
    #                 sink='metadata-rest')

    # metadata_collector_execute(system_name=sys.argv[1], sink="file")

