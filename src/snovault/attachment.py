import json
from base64 import b64decode
from hashlib import md5
from io import BytesIO
from mimetypes import guess_type
from PIL import Image
from pyramid.httpexceptions import (
    HTTPNotFound,
)
from pyramid.response import Response
from pyramid.traversal import find_root
from pyramid.view import view_config
from urllib.parse import (
    quote,
    unquote,
)
from snovault import (
    BLOBS,
    Item,
)
from .validation import ValidationFailure
import magic
import mimetypes


DEGREE_FLIP_MAP = {
    2: (None, True),
    3: (180, None),
    4: (180, True),
    5: (270, True),
    6: (270, None),
    7: (90, True),
    8: (90, None),
}


Image.MAX_IMAGE_PIXELS = 500000000


def includeme(config):
    config.scan(__name__)


class InternalRedirect(Response):
    @staticmethod
    def _make_location_absolute(environ, value):
        # Avoid changing Location header so Apache internally redirects.
        return value


def parse_data_uri(uri):
    if not uri.startswith('data:'):
        raise ValueError(uri)
    meta, data = uri[len('data:'):].split(',', 1)
    meta = meta.split(';')
    mime_type = meta[0] or None
    charset = None
    is_base64 = False
    for part in meta[1:]:
        if part == 'base64':
            is_base64 = True
            continue
        if part.startswith('charset='):
            charset = part[len('charset='):]
            continue
        raise ValueError(uri)

    if is_base64:
        data = b64decode(data)
    else:
        data = unquote(data)

    return mime_type, charset, data


def mimetypes_are_equal(m1, m2):
    major1 = m1.split('/')[0]
    major2 = m2.split('/')[0]
    if major1 == 'text' and major2 == 'text':
        return True
    # Treat application/x-gzip and application/gzip as equivalent.
    if m1.endswith('gzip') and m2.endswith('gzip'):
        return True
    return m1 == m2


class ItemWithAttachment(Item):
    """ Item base class with attachment blob
    """

    def _process_downloads(self, prop_name, properties, downloads):
        attachment = properties[prop_name]
        href = attachment['href']

        if not href.startswith('data:'):
            msg = "Expected data URI."
            raise ValidationFailure('body', [prop_name, 'href'], msg)

        properties[prop_name] = attachment = attachment.copy()
        download_meta = downloads[prop_name] = {}

        try:
            mime_type, charset, data = parse_data_uri(href)
        except (ValueError, TypeError):
            msg = 'Could not parse data URI.'
            raise ValidationFailure('body', [prop_name, 'href'], msg)
        if charset is not None:
            download_meta['charset'] = charset

        # Make sure the file extensions matches the mimetype
        download_meta['download'] = filename = attachment['download']
        mime_type_from_filename, encoding_from_filename = mimetypes.guess_type(filename)
        # Set gzip encoding as mimetype.
        if encoding_from_filename == 'gzip':
            mime_type_from_filename = 'application/gzip'
        if mime_type_from_filename is None:
            mime_type_from_filename = 'application/octet-stream'
        if mime_type:
            if not mimetypes_are_equal(mime_type, mime_type_from_filename):
                raise ValidationFailure(
                    'body', [prop_name, 'href'],
                    'Wrong file extension for %s mimetype.' % mime_type)
        else:
            mime_type = mime_type_from_filename

        # Magic python package depends on BSD 'file' command
        #  Valid json with empty objects, i.e. {} or [],
        #  do not return as valid in 'file'.
        json_file_loaded = False
        if mime_type_from_filename == 'application/json':
            try:
                _ = json.loads(data)
                json_file_loaded = True
            except json.JSONDecodeError:
                raise ValidationFailure(
                    'body', [prop_name, 'href'],
                    f"JSON could not be parsed"
                )

        # Make sure the mimetype appears to be what the client says it is
        # for python3 magic.from_buffer always returns a string a thus doesn't need to be decoded
        try:
            mime_type_detected = magic.from_buffer(data, mime=True).decode('utf-8')
        except AttributeError:
            mime_type_detected = magic.from_buffer(data, mime=True)

        if not json_file_loaded and not mimetypes_are_equal(mime_type, mime_type_detected):
            msg = "Incorrect file type. (Appears to be %s)" % mime_type_detected
            raise ValidationFailure('body', [prop_name, 'href'], msg)

        attachment['type'] = mime_type
        if mime_type is not None:
            download_meta['type'] = mime_type

        # Make sure mimetype is not disallowed
        try:
            allowed_types = self.schema['properties'][prop_name]['properties']['type']['enum']
        except KeyError:
            pass
        else:
            if mime_type not in allowed_types:
                raise ValidationFailure(
                    'body', [prop_name, 'href'], 'Mimetype %s is not allowed.' % mime_type)

        # Validate md5 sum
        md5sum = md5(data).hexdigest()
        if 'md5sum' in attachment and attachment['md5sum'] != md5sum:
            raise ValidationFailure(
                'body', [prop_name, 'md5sum'], 'MD5 checksum does not match uploaded data.')
        else:
            download_meta['md5sum'] = attachment['md5sum'] = md5sum

        # Validate images and store height/width and update md5sum if needed
        major, minor = mime_type.split('/')
        if major == 'image' and minor in ('png', 'gif', 'tiff'):
            stream = BytesIO(data)
            img = Image.open(stream)
            img.verify()
            attachment['width'], attachment['height'] = img.size
        elif major == 'image' and minor == 'jpeg':
            stream = BytesIO(data)
            img = Image.open(stream)
            try:
                orientation = img._getexif()[274]
            except Exception:
                # Maybe this image doesn't use exif data,
                # or it does but the orientation field is absent.
                orientation = 0
            degrees, flip = DEGREE_FLIP_MAP.get(orientation, (None, None))
            transformed = False
            if degrees:
                img = img.rotate(degrees, expand=True)
                transformed = True
            if flip:
                img = img.transpose(Image.FLIP_LEFT_RIGHT)
                transformed = True
            if transformed:
                new_stream = BytesIO()
                img.save(new_stream, format=minor)
                data = new_stream.getvalue()
                download_meta['md5sum'] = attachment['md5sum'] = md5(data).hexdigest()
            img.verify()
            attachment['width'], attachment['height'] = img.size

        registry = find_root(self).registry
        registry[BLOBS].store_blob(data, download_meta)

        attachment['href'] = '@@download/%s/%s' % (prop_name, quote(filename))

    def _update(self, properties, sheets=None):
        changed = []
        unchanged = []
        removed = []
        for prop_name, prop in self.schema['properties'].items():
            if not prop.get('attachment', False):
                continue

            if prop_name not in properties:
                if prop_name in self.propsheets.get('downloads', {}):
                    removed.append(prop_name)
                continue

            attachment = properties[prop_name]
            if 'href' not in attachment:
                msg = "Expected data uri or existing uri."
                raise ValidationFailure('body', [prop_name, 'href'], msg)

            href = attachment['href']
            if href.startswith('@@download/'):
                try:
                    existing = self.properties[prop_name]['href']
                except KeyError:
                    existing = None
                if existing != href:
                    msg = "Expected data uri or existing uri."
                    raise ValidationFailure('body', [prop_name, 'href'], msg)
                unchanged.append(prop_name)
            else:
                changed.append(prop_name)

        if changed or removed:
            properties = properties.copy()
            sheets = {} if sheets is None else sheets.copy()
            sheets['downloads'] = downloads = {}

            for prop_name in unchanged:
                downloads[prop_name] = self.propsheets['downloads'][prop_name]

            for prop_name in changed:
                self._process_downloads(prop_name, properties, downloads)

        super(ItemWithAttachment, self)._update(properties, sheets)


@view_config(name='download', context=ItemWithAttachment, request_method='GET',
             permission='view', subpath_segments=2)
def download(context, request):
    prop_name, filename = request.subpath
    downloads = context.propsheets['downloads']
    try:
        download_meta = downloads[prop_name]
    except KeyError:
        raise HTTPNotFound(prop_name)

    if download_meta['download'] != filename:
        raise HTTPNotFound(filename)

    mimetype, content_encoding = guess_type(filename, strict=False)
    if content_encoding == 'gzip':
        mimetype = 'application/gzip'
    if mimetype is None:
        mimetype = 'application/octet-stream'

    # If blob is external, serve via proxy using X-Accel-Redirect
    blob_storage = request.registry[BLOBS]
    accel_redirect_header = request.registry.settings.get('accel_redirect_header')
    if hasattr(blob_storage, 'get_blob_url') and accel_redirect_header:
        blob_url = blob_storage.get_blob_url(download_meta)
        return InternalRedirect(headers={accel_redirect_header: '/_proxy/' + str(blob_url)})

    # Otherwise serve the blob data ourselves
    blob = request.registry[BLOBS].get_blob(download_meta)
    headers = {
        'Content-Type': mimetype,
    }
    return Response(body=blob, headers=headers)
