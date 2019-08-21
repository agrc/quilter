import os
import sys

import quilter


def test(method, csv_list, output_root):
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


def main(args):

    output_dir = args[1]
    script_dir = os.path.dirname(os.path.abspath(__file__))
    csv_dir = os.path.join(script_dir, 'csvs')
    print(csv_dir)
    test_csvs = []
    for dir_name, subdir_list, file_list in os.walk(csv_dir):
        for fname in file_list:
            if str.endswith(fname, '.csv'):
                test_csvs.append(os.path.join(dir_name, fname))
    
    #: Change the first argument as desired to test downloading only, dl + merge, and dl + reproject
    #: Second arg is list of csvs
    #: Third arg is output directory (suggest calling like: 'python tests.py .' to put in current directory)
    #:      working directory becomes output_dir/method/csv_name/ (c:\temp\dl\10mDEMS\q_zips, etc)
    test('reproject_only', test_csvs, output_dir)

if __name__ == '__main__':
    main(sys.argv)
