import os

import quilter


def test(method, csv_list):
    output_root = r'c:\gis\elevation\statewide\test'
    for csv_path in csv_list:
        
        csv_name = os.path.basename(csv_path).split('.')[0]
        
        if method == 'download':
            test_dir = os.path.join(output_root, 'dl', csv_name)
            args = [csv_path, test_dir]
        
        elif method == 'merge':
            test_dir = os.path.join(output_root, 'merge', csv_name)
            test_name = csv_name.split('.')[0]
            args = [csv_path, test_dir, '-m', test_name]
        
        elif method == 'reproject':
            test_dir = os.path.join(output_root, 'reproject', csv_name)
            test_name = csv_name.split('.')[0]
            args = [csv_path, test_dir, '-m', test_name, '-p', 'EPSG:3566']
        
        elif method == 'reproject_only':
            test_dir = os.path.join(output_root, 'reproject_only', csv_name)
            args = [csv_path, test_dir, '-p', 'EPSG:3566']

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
    test('reproject_only', test_csvs)

if __name__ == '__main__':
    main()
