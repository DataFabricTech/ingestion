import argparse
import sys
from metadata_collector import metadata_collector_execute
from metadata_profiler import metadata_profiler_execute

ALL_PARAMS = ['database', 'schema', 'table', 'bucket', 'object']

def parse_key_value_list(kv_list):
    result = {}
    for item in kv_list or []:
        if '=' not in item:
            raise ValueError(f"인자 '{item}'은 'key=value' 형식이어야 합니다.")
        key, value = item.split('=', 1)
        if key not in ALL_PARAMS:
            raise ValueError(f"'{key}'는 허용되지 않는 인자입니다. ({ALL_PARAMS})")

        if key in result:
            result[key].append(value)
        else:
            result[key] = [value]

    if len(result) > 0:
        return result
    else:
        return None

def main():

    parser = argparse.ArgumentParser(description="", epilog="* include, exclude 설정 가능한 인자 : database, schema, table, bucket, object")

    parser.add_argument('--collector_type', required=True, help='수집 타입 (필수) [ingestion, profile]')
    parser.add_argument('--system_id', required=True, help='시스템 ID (필수)')

    parser.add_argument('--include', nargs='*', help="포함할 인자들 (예: schema=public table=users)")
    parser.add_argument('--exclude', nargs='*', help="제외할 인자들 (예: bucket=data)")

    args = parser.parse_args()

    try:
        filter_include_dict = parse_key_value_list(args.include)
        filter_exclude_dict = parse_key_value_list(args.exclude)
    except ValueError as e:
        print(e)
        parser.print_help()
        sys.exit(1)

    if args.collector_type == 'ingestion':
        # metadata_collector_execute(system_id=args.system_id, sink='metadata-rest',
        #                            filter_include_dict=filter_include_dict, filter_exclude_dict=filter_exclude_dict)

        # metadata_collector_execute(system_id='109ae637-9e13-44f9-9686-a79cf1e12499',
        #                 sink='metadata-rest')

        # metadata_collector_execute(system_id='08c36eb4-9cca-4a08-b40e-b9d26d3cc174',
        #                 sink='metadata-rest')

        metadata_collector_execute(system_id=args.system_id, sink='file',
                                   filter_include_dict=filter_include_dict, filter_exclude_dict=filter_exclude_dict)
    elif args.collector_type == 'profile':
        metadata_profiler_execute(system_id=args.system_id, sink='metadata-rest',
                                filter_include_dict=filter_include_dict, filter_exclude_dict=filter_exclude_dict)

        # metadata_profiler_execute(system_id='08c36eb4-9cca-4a08-b40e-b9d26d3cc174',
        #                sink='metadata-rest')

    else:
        parser.print_help()
        sys.exit(1)



if __name__ == "__main__":
    main()


