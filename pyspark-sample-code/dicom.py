
# pip install numpy
# pip install pydicom
import pydicom
from pydicom import encaps
# pip install pillow
from PIL import Image

# install GDCM
#   get site-packages directory
#       python3 -m site --user-site
#       [path/to/site-packages]
#   sudo apt install python3-gdcm
#   find /usr -name gdcm.py
#       [path/to/package]/gdcm.py
#   find /usr -name libgdcmCommon.so.*
#       [path/to/so/files]/libgdcmCommon.so.3.0
#   cp [path/to/package]/gdcm.py [path/to/site-packages]/
#   cp [path/to/package]/gdcmswig.py [path/to/site-packages]/
#   cp [path/to/package]/_gdcmswig*.so [path/to/site-packages]/
#   cp [path/to/so/files]/libgdcm* [path/to/site-packages]/


import io


startPoint = [25, 115]
endPoint = [445, 215]

def main():
    dcmfile = '693_UNCI.dcm'

    ds = pydicom.dcmread(f'./dcm/{dcmfile}', force=True)

    if not hasattr(ds.file_meta, 'TransferSyntaxUID'):
        ds.file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.1'  # Explicit VR Little Endian
    # print('\t', ds.file_meta.TransferSyntaxUID)

    ps = ds.pixel_array
    # print(ps)
    # print(ps.shape)
    # print(ps[25][15])

    for yy in range(startPoint[0], endPoint[0]):
        for xx in range(startPoint[1], endPoint[1]):
            ps[yy][xx] = 30000 if ds.SamplesPerPixel == 1 else [3, 6, 7]

    if not ds.file_meta.TransferSyntaxUID.startswith('1.2.840.10008.1.2.4.'):
        ds.PixelData = ps.tobytes()
    else:
        f = io.BytesIO()
        img = Image.fromarray(ps)
        img.save(f, format='jpeg2000', quality_mode='dB')
        ds.file_meta.TransferSyntaxUID = '1.2.840.10008.1.2.4.90'   # JPEG 2000 Image Compression (Lossless Only)
        ds.PixelData = encaps.encapsulate([f.getvalue()])

    ds.save_as(f'./dcm_modified/{dcmfile}')


###########################################
if __name__ == "__main__":
    main()
