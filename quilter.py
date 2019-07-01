'''
quilter.py

Download ALL THE RASTERS!!!

'''

import csv
import os
import tempfile
import zipfile
import shutil
import sys
import inspect
import subprocess
import argparse
from pathlib import Path

import requests
from osgeo import gdal, ogr, osr


#: Progress bar for download
#: modified from https://sumit-ghosh.com/articles/python-download-progress-bar/
def progbar(progress, total):
    '''
    Simple output for reporting progress given current progress value and progress value at completion
    '''
    done = int(50 * progress / total)
    percent = round(100 * progress / total, 2)
    sys.stdout.write('\r[{}{}] {}%'.format('#' * done, '_' * (50 - done), percent))
    sys.stdout.flush()


#: Shamelessly modified from Sumit Ghosh,
#: https://sumit-ghosh.com/articles/python-download-progress-bar/
def download(url, filename):
    '''
    Downloades url to filename using requests, reports via progress bar
    '''
    with open(filename, 'wb') as f:
        response = requests.get(url, stream=True)
        total = response.headers.get('content-length')

        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total / 1000), 1024 * 1024)):
                downloaded += len(data)
                f.write(data)
                progbar(downloaded, total)
    sys.stdout.write('\n')


#: GDAL callback method that seems to work, as per
#: https://gis.stackexchange.com/questions/237479/using-callback-with-python-gdal-rasterizelayer
def gdal_progress_callback(complete, message, unknown):
    '''
    Progress bar styled after the default GDAL progress bars. Uses specific signature to conform with GDAL core.
    '''
    #: 40 stops on our progress bar, so scale to 40
    done = int(40 * complete / 1)

    #: Build string: 0...10...20... - done.
    status = ''
    for i in range(0, done):
        if i % 4 == 0:
            status += str(int(i / 4 * 10))
        else:
            status += '.'
    if done == 40:
        status += '100 - done.\n'

    sys.stdout.write('\r{}'.format(status))
    sys.stdout.flush()
    return 1


def read_csv(csv_path):
    links = []
    with open(csv_path, 'r') as cfile:
        reader = csv.reader(cfile)
        for row in reader:
            if str.startswith(row[6], 'http'):
                links.append((row[2], row[6]))

    return links


def download_links(link_list, save_dir):
    d_prog = 0
    for link in link_list:
        fname = link.split('/')[-1]
        d_prog += 1
        print('Downloading {}, {} of {}'.format(fname, d_prog, str(len(link_list))))
        outpath = os.path.join(save_dir, fname)
        download(link, outpath)


def extract_files(source_dir, unzip_dir):
    zip_list = []
    z_prog = 0
    for dir_name, subdir_list, file_list in os.walk(source_dir):
        for fname in file_list:
            if str.endswith(fname, '.zip'):
                zip_list.append(os.path.join(dir_name, fname))

    for z in zip_list:
        z_prog += 1
        print('Extracting {}, {} of {}'.format(z, z_prog, str(len(zip_list))))
        with zipfile.ZipFile(z) as zip_ref:
            zip_ref.extractall(unzip_dir)


def copy_extracted_files(extensions, source_dir, target_dir):
    extract_list = []
    extract_prog = 0
    for dir_name, subdir_list, file_list in os.walk(source_dir):
        for fname in file_list:
            if str.endswith(fname, extensions):
                extract_list.append(os.path.join(dir_name, fname))

    for extract_file in extract_list:
        extract_prog += 1
        f_name = os.path.basename(extract_file)
        print('Copying {}, {} of {}'.format(f_name, extract_prog, str(len(extract_list))))
        shutil.copy2(extract_file, target_dir)


def raster_merge(raster_folder, output_location, temp_vrt_path, extensions, crs):

    #: Merge files by building VRT
    vrt_list = []
    for dir_name, subdir_list, file_list in os.walk(raster_folder):
        for fname in file_list:
            if str.endswith(fname, extensions):
                vrt_list.append(os.path.join(dir_name, fname))

    vrt_opts = gdal.BuildVRTOptions(resampleAlg='cubic')
    vrt = gdal.BuildVRT(temp_vrt_path, vrt_list, options=vrt_opts)
    vrt = None  # releases file handle

    creation_opts = ['bigtiff=yes', 'compress=lzw', 'tiled=yes']

    #: If the source is jpeg compressed, change output compression to jpeg
    if vrt_list:
        sample_dataset = gdal.Open(vrt_list[0], gdal.GA_ReadOnly)
        sample_metadata = sample_dataset.GetMetadata('IMAGE_STRUCTURE')

        if 'COMPRESSION' in sample_metadata and 'JPEG' in sample_metadata['COMPRESSION']:
            creation_opts = ['bigtiff=yes', 'compress=jpeg', 'tiled=yes']
            gdal.SetConfigOption('COMPRESS_OVERVIEW', 'JPEG')

            if 'YCbCr' in sample_metadata['COMPRESSION']:
                creation_opts.append('photometric=ycbcr')
                gdal.SetConfigOption('PHOTOMETRIC_OVERVIEW', 'YCBCR')
        
        sample_dataset = None

    #: Project if desired
    if crs:
        print('\nProjecting {} to {}'.format(output_location, crs))
        warp_opts = gdal.WarpOptions(dstSRS=crs,
                                     resampleAlg='cubic',
                                     format='GTiff',
                                     multithread=True,
                                     creationOptions=creation_opts,
                                     callback=gdal_progress_callback)
        dataset = gdal.Warp(output_location, temp_vrt_path, options=warp_opts)
        dataset = None  #: Releases file handle

        print('\nBuilding overviews...')
        dataset = gdal.Open(output_location, gdal.GA_ReadOnly)
        dataset.BuildOverviews('NEAREST', [2, 4, 8, 16], gdal_progress_callback)
        dataset = None

    #: Otherwise, just translate vrt to tif
    else:
        print('\nMerging into {}'.format(output_location))
        trans_opts = gdal.TranslateOptions(format='GTiff',
                                           creationOptions=creation_opts,
                                           callback=gdal_progress_callback)
        dataset = gdal.Translate(output_location, temp_vrt_path, options=trans_opts)
        dataset = None

        print('\nBuilding overviews...')
        dataset = gdal.Open(output_location, gdal.GA_ReadOnly)
        dataset.BuildOverviews('NEAREST', [2, 4, 8, 16], gdal_progress_callback)
        dataset = None


def vector_merge(shp_folder, output_location):
    '''
    Merges all shapefiles in shp_folder and saves to output_location.
    '''
    
    shp_list = []

    #: Assumes gdal installed via Conda; unknown if this works with OSG4W, qgis, etc.
    #: Really, if you're doing GDAL in python, you should use conda. Seriously.
    gdal_path = Path(inspect.getfile(gdal))
    if r'Program Files\ArcGIS' in str(gdal_path):
        raise RuntimeError(
            'The GDAL environment bundled with ArcGIS Pro does not provide the script necessary for merging ' +
            'shapefiles. Recommend installing latest GDAL from the conda-forge channel: conda install -c ' +
            'conda-forge gdal.'
        )
    else:
        merge_path = gdal_path.parents[3] / 'Scripts' / 'ogrmerge.py'

    if not os.path.exists(merge_path):
        raise FileNotFoundError('ogrmerge.py not found where expected. Did you install GDAL from conda-forge?')

    shp_args = ['python', merge_path, '-o', output_location]

    for dir_name, subdir_list, file_list in os.walk(shp_folder):
        for fname in file_list:
            if str.endswith(fname, '.shp'):
                shp_list.append(os.path.join(dir_name, fname))

    subprocess.run(shp_args)


def main():
    #: User Input:
    #: csv file
    #: destination folder
    #: -m merge flag and name
    #: -p project flag and accompanying EPSG:xxxxx designator

    parser = argparse.ArgumentParser(description='Download helper for raster.utah.gov')

    parser.add_argument('csv', help='CSV file of download links.')
    parser.add_argument('destination', help='Directory for extracted files (must already exist).')

    parser.add_argument(
        '-m',
        '--merge',
        dest='name',
        help=
        'Merge downloaded files to specified base name. .tif or .shp extensions will be added as appropriate. Requires GDAL.'
    )
    parser.add_argument('-p',
                        '--project',
                        dest='crs',
                        help='Reproject merged file to specified CRS. Specify CRS like EPSG:x or ESRI:x. Requires -m.')

    args = parser.parse_args()

    #: Set defaults
    merge = False
    project = False
    delete = False

    outfolder = args.destination
    csv_file = args.csv
    if args.name:
        final_name = args.name
        merge = True
    
    if args.crs:
        projection = args.crs
        # project = True
    else:
        projection = None

    ext_list = ('.img', '.tif', '.asc', '.jpg', '.jgw', '.xyz', '.tfw', '.dbf', '.prj', '.shp', '.shx')
    raster_exts = ('.img', '.tif', '.asc', '.jpg')

    try:

        #: set up temporary directory
        temp_dir = tempfile.mkdtemp()

        #: outfile sanity checks
        #: appends pid to path to give better chance for unique name
        extract_folder = os.path.join(outfolder, 'extracted' + str(os.getpid()))
        if os.path.exists(extract_folder):
            raise IOError(
                'Extrated files directory {} already exists. Usually just trying to run the script again should fix this problem.'
                .format(extract_folder))

        #: Do these checks now so that they don't download files only to
        #: bomb out at the end


        #: Projection requires merging
        if project and not merge:
            raise ValueError('Must specify merged file name with -m.')

        #: Checks if gdal installed, proper projection code
        if merge:
            gdal.UseExceptions()
            osr.UseExceptions()
            gdal.SetConfigOption('COMPRESS_OVERVIEW', 'DEFLATE')
            if project:
                proj_code = projection.split(':')
                reference = osr.SpatialReference()
                if proj_code[0].upper() == 'ESRI':
                    reference.ImportFromESRI(int(proj_code[1]))
                elif proj_code[0].upper() == 'EPSG':
                    reference.ImportFromEPSG(int(proj_code[1]))
                reference = None

        #: TODO: updated csv format from raster.utah.gov app
        #: TODO: Framework for handling different csv formats (which column
        #:       the line is in)

        #: Create list of links
        print('\nReading CSV...')
        dl_links = read_csv(csv_file)


        #: TODO: Test this check
        if "SHP" in dl_links[0]:
            raster = False
        else:
            raster = True

        #: Explicit file exists checks
        if raster:
            raster_outpath = os.path.join(outfolder, final_name + '.tif')
            vrt_path = os.path.join(temp_dir, final_name + str(os.getpid()) + '.vrt')
            if os.path.exists(raster_outpath):
                raise IOError('Output file {} already exists.'.format(raster_outpath))
        if not raster:
            vector_outpath = os.path.join(outfolder, final_name + '.shp')
            if os.path.exists(vector_outpath):
                raise IOError('Output file {} already exists.'.format(vector_outpath))

        #: Download links to temp dir
        print('\nDownloading files...')
        dl_folder = os.path.join(temp_dir, 'dl')
        os.mkdir(dl_folder)
        download_links(dl_links, dl_folder)

        #: Unzip to temp dir
        print('\nUnzipping files...')
        unzip_folder = os.path.join(temp_dir, 'uz')
        os.mkdir(unzip_folder)
        extract_files(dl_folder, unzip_folder)

        #: Copy out all relevant files to output dir
        print('\nCopying extracted files...')
        os.mkdir(extract_folder)
        copy_extracted_files(ext_list, unzip_folder, extract_folder)

        #: If we've gotten this far, we can go ahead and delete the downloaded zips when we're finished.
        delete = True

        #: Raster merging
        if merge and raster:
            raster_merge(extract_folder, raster_outpath, vrt_path, raster_exts, projection)

        #: TODO: vector reprojection?
        #: Check out ogr2ogr, may have to subprocess

        #: Shapefile merging
        elif merge and not raster:
            vector_merge(extract_folder, vector_outpath)



    except ImportError as e:
        print("\n===========\nDON'T PANIC\n===========")
        if 'gdal' in e.args[0]:
            print(
                "Can't import GDAL python bindings. Recommend installing latest GDAL from the conda-forge channel via conda: conda install -c conda-forge gdal."
            )
        print('\nPython error message:')
        print(e)
        delete = True

    except RuntimeError as e:
        print("\n===========\nDON'T PANIC\n===========")
        if 'proj_create_from_database' in e.args[0]:
            print('Projection code not recognized. Must be a valid EPSG or ESRI code in the format EPSG:xxxx or ESRI:xxxx.')
            delete = True
        else:
            print('Whoops, something went wrong. Any finished downloads have been left in {}'.format(temp_dir))
            delete = False
        print('\nPython error message:')
        print(e)

    except Exception as e:
        print("\n===========\nDON'T PANIC\n===========")
        print('Whoops, something went wrong. Any finished downloads have been left in {}'.format(temp_dir))
        print('\nPython error message:')
        print(e)
        delete = False

    finally:
        #: Clean up temp directory
        if delete:
            shutil.rmtree(temp_dir)

if __name__ == '__main__':
    main()