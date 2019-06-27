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
import requests
import traceback
import inspect
import subprocess
import argparse
from pathlib import Path

#: User Input:
#: csv file
#: destination folder
#: -m merge flag and name
#: -p project flag and accompanying EPSG:xxxxx designator
#: -d don't compress tif (ignore for aerial imagery)

parser = argparse.ArgumentParser(description='Download helper for\
                                              raster.utah.gov')

parser.add_argument('csv', help='CSV file of download links.')
parser.add_argument('destination', 
                    help='Directory for extracted files (must already exist).')

parser.add_argument('-m', '--merge', dest='name', 
                    help='Merge downloaded files to specified base name. .tif\
                    or .shp extensions will be added as appropriate. Requires\
                    GDAL.')
parser.add_argument('-p', '--project', dest='crs', help='Reproject merged file\
                    to specified CRS. Specify CRS like EPSG:x or ESRI:x.\
                    Requires -m.')
parser.add_argument('-d', '--dont_compress', help='Disable LZW compression of\
                    merged raster (all aerials will remain compressed).',
                    action='store_true')

args = parser.parse_args()

#: Set defaults
merge = False
project = False
compress = True
delete = False

outfolder = args.destination
csv_file = args.csv
if args.name:
    final_name = args.name
    merge = True
if args.crs:
    projection = args.crs
    project = True
if args.dont_compress:
    compress = False

# outfolder = 'c:\\gis\\elevation\\statewide\\test'
# csv_file = 'c:\\users\\jdadams\\downloads\\10mDEMS2.csv'
# projection = 'EPSG:26912'
# final_name = '10m'

ext_list = ('.img', '.tif', '.asc', '.jpg', '.jgw', '.xyz', '.tfw', '.dbf', 
            '.prj', '.shp', '.shx')
raster_exts = ('.img', '.tif', '.asc', '.jpg')

# merge = True
# project = True 
# compress = True  # Default to always compress rasters w/ lzw

#: TODO: Need to set these automagically. Maybe something in CSV?
raster = True
vector = False


#: Progress bar for download
#: modified from https://sumit-ghosh.com/articles/python-download-progress-bar/
def progbar(progress, total):
    done = int(50 * progress / total)
    percent = round(100 * progress / total, 2)
    sys.stdout.write('\r[{}{}] {}%'.format('#' * done, '_' * (50 - done),
                                           percent))
    sys.stdout.flush()


#: Shamelessly modified from Sumit Ghosh, 
#: https://sumit-ghosh.com/articles/python-download-progress-bar/
def download(url, filename):
    with open(filename, 'wb') as f:
        response = requests.get(url, stream=True)
        total = response.headers.get('content-length')

        if total is None:
            f.write(response.content)
        else:
            downloaded = 0
            total = int(total)
            for data in response.iter_content(chunk_size=max(int(total/1000), 1024*1024)):
                downloaded += len(data)
                f.write(data)
                progbar(downloaded, total)
    sys.stdout.write('\n')


#: GDAL callback method that seems to work, as per
#: https://gis.stackexchange.com/questions/237479/using-callback-with-python-gdal-rasterizelayer
def gdal_progress_callback(complete, message, unknown):
    #: 40 stops on our progress bar, so scale to 40
    done = int(40 * complete / 1)

    #: Build string: 0...10...20... - done.
    status = ''
    for i in range(0, done):
        if i%4 == 0:
            status += str(int(i/4*10))
        else:
            status += '.'
    if done == 40:
        status += '100 - done.\n'

    sys.stdout.write('\r{}'.format(status))
    sys.stdout.flush()
    return 1


try:

    #: set up temporary directory
    temp_dir = tempfile.mkdtemp()

    #: outfile sanity checks
    #: appends pid to path to give better chance for unique name
    extract_folder = os.path.join(outfolder, 'extracted' + str(os.getpid()))
    if os.path.exists(extract_folder):
        raise IOError('Extrated files directory {} already exists. Usually just trying to run the script again should fix this problem.'.format(extract_folder))

    #: Do these checks now so that they don't download files only to 
    #: bomb out at the end
    #: Explicit file exists checks
    if raster:
        raster_outpath = os.path.join(outfolder, final_name + '.tif')
        if os.path.exists(raster_outpath):
            raise IOError('Output file {} already exists.'.format(
                          raster_outpath))
    if vector:
        vector_outpath = os.path.join(outfolder, final_name + '.shp')
        if os.path.exists(vector_outpath):
            raise IOError('Output file {} already exists.'.format(
                          vector_outpath))

    #: Projection requires merging
    if project and not merge:
        raise ValueError('Must specify merged file name with -m.')

    #: Checks if gdal installed, proper projection code
    if merge:
        from osgeo import gdal, osr
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
    print('Reading CSV...')
    dl_links = []
    with open(csv_file, 'r') as cfile:
        reader = csv.reader(cfile)
        for row in reader:
            if str.startswith(row[6], 'http'):
                dl_links.append(row[6]) 

    #: Download links to temp dir
    print('Preparing to download files...')
    dl_folder = os.path.join(temp_dir, 'dl')
    os.mkdir(dl_folder)
    progress = 0
    for link in dl_links:
        fname = link.split('/')[-1]
        progress += 1
        print('Downloading {}, {} of {}'.format(fname, progress, 
                                                str(len(dl_links))))
        outpath = os.path.join(dl_folder, fname)
        download(link, outpath)

    #: Unzip to temp dir
    print('\nUnzipping files...')
    unzip_folder = os.path.join(temp_dir, 'uz')
    os.mkdir(unzip_folder)
    zip_list = []
    z_prog = 0
    for dir_name, subdir_list, file_list in os.walk(dl_folder):
        for fname in file_list:
            if str.endswith(fname, '.zip'):
                zip_list.append(os.path.join(dir_name, fname))

    for z in zip_list:
        z_prog += 1
        print('Extracting {}, {} of {}'.format(z, z_prog, str(len(zip_list))))
        with zipfile.ZipFile(z) as zip_ref:
            zip_ref.extractall(unzip_folder)

    #: Copy out all .img to output dir
    print('\nCopying extracted files...')
    os.mkdir(extract_folder)
    extract_list = []
    extract_prog = 0
    for dir_name, subdir_list, file_list in os.walk(unzip_folder):
        for fname in file_list:
            if str.endswith(fname, ext_list):
                extract_list.append(os.path.join(dir_name, fname))

    for f in extract_list:
        extract_prog += 1
        f_name = os.path.basename(f)
        print('Copying {}, {} of {}'.format(f_name, extract_prog, 
                                            str(len(extract_list))))
        shutil.copy2(f, extract_folder)

    #: If we've gotten this far, we can go ahead and delete the downloaded 
    #: zips when we're finished.
    delete = True

    #: Shapefile merging
    if merge and vector:
        shp_list = []

        #: Assumes gdal installed via Conda; unknown if this works with 
        #: OSG4W, qgis, etc.
        #: Really, if you're doing GDAL in python, you should use conda. 
        #: Seriously.
        gdal_path = Path(inspect.getfile(gdal))
        if r'Program Files\ArcGIS' in str(gdal_path):
            raise RuntimeError('The GDAL environment bundled with ArcGIS Pro does not provide the script necessary for merging shapefiles. Recommend installing latest GDAL from the conda-forge channel: conda install -c conda-forge gdal.')
        else: 
            merge_path = gdal_path.parents[3] / 'Scripts' / 'ogrmerge.py'

        if not os.path.exists(merge_path):
            raise FileNotFoundError('ogrmerge.py not found where expected. Did you install GDAL from conda-forge?')

        shp_args = ['python', merge_path, '-o', vector_outpath]

        for dir_name, subdir_list, file_list in os.walk(extract_folder):
            for fname in file_list:
                if str.endswith(fname, '.shp'):
                    shp_list.append(os.path.join(dir_name, fname))

        subprocess.run(shp_args)
    
    #: Raster merging
    elif merge and raster:
        print('\nCombining into one dataset...')

        #: Merge files by building VRT
        vrt_list = []
        vrt_path = os.path.join(temp_dir, final_name + str(os.getpid()) +
                                '.vrt')
        for dir_name, subdir_list, file_list in os.walk(extract_folder):
            for fname in file_list:
                if str.endswith(fname, raster_exts):
                    vrt_list.append(os.path.join(dir_name, fname))

        vrt_opts = gdal.BuildVRTOptions(resampleAlg='cubic')
        vrt = gdal.BuildVRT(vrt_path, vrt_list, options=vrt_opts)
        vrt = None  # releases file handle

        if compress:
            creation_opts=['bigtiff=yes', 'compress=lzw', 'tiled=yes']
        else:
            creation_opts=['tiled=yes']

        #: Overwrite creation options for jpeg compression of aerial imagery
        if len(vrt_list) > 0:
            ds = gdal.Open(vrt_list[0], gdal.GA_ReadOnly)
            md = ds.GetMetadata('IMAGE_STRUCTURE')
            if 'COMPRESSION' in md and 'JPEG' in md['COMPRESSION']:
                creation_opts = ['bigtiff=yes', 'compress=jpeg', 'tiled=yes']
                gdal.SetConfigOption('COMPRESS_OVERVIEW', 'JPEG')
                if 'YCbCr' in md['COMPRESSION']:
                    creation_opts.append('photometric=ycbcr')
                    gdal.SetConfigOption('PHOTOMETRIC_OVERVIEW', 'YCBCR')

        #: Project if desired
        if project:
            print('\nProjecting {} to {}'.format(raster_outpath, projection))
            warp_opts = gdal.WarpOptions(dstSRS=projection, 
                                         resampleAlg='cubic', 
                                         format='GTiff', 
                                         multithread=True,
                                         creationOptions=creation_opts,
                                         callback=gdal_progress_callback)
            dataset = gdal.Warp(raster_outpath, vrt_path, options=warp_opts)
            dataset = None  #: Releases file handle

            print('\nBuilding overviews...')
            dataset = gdal.Open(raster_outpath, gdal.GA_ReadOnly)
            dataset.BuildOverviews('NEAREST', [2, 4, 8, 16], 
                                   gdal_progress_callback)
            dataset = None
        
        #: Otherwise, just translate vrt to tif
        else:
            print('\nMerging into {}'.format(raster_outpath))
            trans_opts = gdal.TranslateOptions(format='GTiff', 
                                               creationOptions=creation_opts, 
                                               callback=gdal_progress_callback)
            dataset = gdal.Translate(raster_outpath, vrt_path, 
                                     options=trans_opts)
            dataset = None

            print('\nBuilding overviews...')
            dataset = gdal.Open(raster_outpath, gdal.GA_ReadOnly)
            dataset.BuildOverviews('NEAREST', [2, 4, 8, 16], 
                                   gdal_progress_callback)
            dataset = None
    
except ImportError as e:
    print("\n===========\nDON'T PANIC\n===========")
    if 'gdal' in e.args[0]:
        print("Can't import GDAL python bindings. Recommend installing latest GDAL from the conda-forge channel via conda: conda install -c conda-forge gdal.")
    print('\nPython error message:')
    print(e)
    # traceback.print_exc()
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
    # traceback.print_exc()

except Exception as e:
    print("\n===========\nDON'T PANIC\n===========")
    print('Whoops, something went wrong. Any finished downloads have been left in {}'.format(temp_dir))
    print('\nPython error message:')
    print(e)
    # traceback.print_exc()
    delete = False

finally:    
    #: Clean up temp directory
    if delete:
        shutil.rmtree(temp_dir)