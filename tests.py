import os

import quilter


def test(method, csv_list):
    output_root = r'c:\gis\elevation\statewide\tests'
    for csv_path in csv_list:
        
        if method == 'download':
            test_dir = os.path.join(output_root, 'dl')
            args = [csv_path, test_dir]
        
        elif method == 'merge':
            test_dir = os.path.join(output_root, 'merge')
            test_name = csv_path.split('.')[0]
            args = ['-m', test_name, csv_path, test_dir]
        
        elif method == 'reproject':
            test_dir = os.path.join(output_root, 'reproject')
            test_name = csv_path.split('.')[0]
            args = ['-m', test_name, '-p', 'EPSG:3566', csv_path, test_dir]
        
        else:
            raise NotImplementedError

        quilter.main(args)


def main():
    csv_dir = r'c:\gis\git\quilter\csvs'
    test_csvs = []
    for dir_name, subdir_list, file_list in os.walk(csv_dir):
        for fname in file_list:
            if str.endswith(fname, '.csv'):
                test_csvs.append(os.path.join(dir_name, fname))
    
    #: Change the first argument as desired to test downloading only, dl + merge, and dl + reproject
    test('download', test_csvs)

if __name__ == '__main__':
    main()
