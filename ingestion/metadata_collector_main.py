import sys
from metadata_collector import metadata_collector_execute

if __name__ == "__main__":
    pass

    # metadata_collector_execute(system_id=sys.argv[1],
    #                 sink='metadata-rest')

    # metadata_collector_execute(system_id='109ae637-9e13-44f9-9686-a79cf1e12499',
    #                 sink='metadata-rest')

    metadata_collector_execute(system_id='minio_test',
                    sink='metadata-rest')

    # metadata_collector_execute(system_name=sys.argv[1], sink="file")
    # metadata_collector_execute(system_id="51de6bd5-085b-4aad-bac2-d35adb44c18b", sink="file")
    # metadata_collector_execute(system_id="tibero_phy", sink="file")
    # metadata_collector_execute(system_id="altibase_phy", sink="file")

