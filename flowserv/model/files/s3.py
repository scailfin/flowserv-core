# This file is part of the Reproducible and Reusable Data Analysis Workflow
# Server (flowServ).
#
# Copyright (C) 2019-2020 NYU.
#
# flowServ is free software; you can redistribute it and/or modify it under the
# terms of the MIT License; see LICENSE file for more details.

"""Implementation of the file store for backends that use S3 buckets (or S3
bucket-like) objects.

upload_file()
"""

import botocore
import os

from io import BytesIO, StringIO
from typing import IO, List, Set, Tuple, TypeVar, Union

from flowserv.model.files.base import FileStore

import flowserv.config.base as config
import flowserv.config.files as fconfig
import flowserv.error as err
import flowserv.util as util


# Type variable for S3 bucket objects.
B = TypeVar('B')


"""Environment variable for unique bucket identifier."""
FLOWSERV_S3BUCKET = 'FLOWSERV_S3BUCKET'


class BucketStore(FileStore):
    """Implementation of the abstract file store class. In this implementation
    all files are maintained on the local file system under a given base
    directory.
    """
    def __init__(self, bucket: B = None):
        """Initialize the storage bucket.

        Parameters
        ----------
        bucket: S3.Bucket
            Object that implements the delete, download, and upload methods of
            the S3.Bucket interface.
        """
        if bucket is None:
            bucket_id = config.get_variable(FLOWSERV_S3BUCKET)
            if bucket_id is None:
                from flowserv.tests.files import DiskBucket
                bucket = DiskBucket()
            else:  # pragma: no cover
                import boto3
                bucket = boto3.resource('s3').Bucket(bucket_id)
        self.bucket = bucket

    def __repr__(self):
        """Get object representation ."""
        return "<BucketStore bucket={} />".format(self.bucket)

    def configuration(self) -> List[Tuple[str, str]]:
        """Get a list of tuples with the names of additional configuration
        variables and their current values.

        Returns
        -------
        list((string, string))
        """
        return [
            (fconfig.FLOWSERV_FILESTORE_CLASS, 'BucketStore'),
            (fconfig.FLOWSERV_FILESTORE_MODULE, 'flowserv.model.files.s3'),
            (FLOWSERV_S3BUCKET, config.get_variable(FLOWSERV_S3BUCKET))
        ]

    def copy_files(self, src: str, files: List[Tuple[str, str]]):
        """Copy a list of files or dirctories from a given source directory on
        the local file system to the file store. The list of files contains
        tuples of relative file source and target path. The source path is
        relative to the given src folder and may reference existing files or
        directories.

        Parameters
        ----------
        src: string
            Path to source directory on disk.
        files: list((string, string))
            List of file source and target path. All path names are relative.
        """
        # Upload files in the given list. Directories cannot be uploaded
        # directly using the buckat interface. Thus, we need to recursively
        # include all files in directories. Exclude entries in the
        # list that do not point to existing files or directories.
        for source, target in files:
            source = os.path.join(src, source)
            if os.path.isfile(source):
                self.bucket.upload_file(source, target)
            elif os.path.isdir(source):
                upload_dir(source=source, target=target, bucket=self.bucket)

    def delete_file(self, key: str):
        """Delete the file with the given key.

        Parameters
        ----------
        key: string
            Unique file key.
        """
        # The key may point to a file or a folder. In case of the latter we
        # need to collect the keys for all objects in the folder.
        keys = find_keys(query=key, bucket=self.bucket)
        # Only class the delete_objects method if the list of matched objects
        # is not empty.
        if len(keys) > 0:
            objects = [{'Key': k} for k in keys]
            self.bucket.delete_objects(Delete={'Objects': objects})

    def download_archive(self, src: str, files: List[str]) -> IO:
        """Download all files in the given list from the specified source
        directory as a tar archive.

        Parameters
        ----------
        src: string
            Relative path to the files source directory.
        files: list(string)
            List of relative paths to files (or directories) in the specified
            source directory. Lists the files to include in the returned
            archive.

        Returns
        -------
        io.BytesIO
        """
        fileobjs = list()
        for source, target in files:
            downloads = download_list(
                source=os.path.join(src, source),
                target=target,
                bucket=self.bucket
            )
            for key, target in downloads:
                fileobjs.append((self.load_file(key), target))
        return util.archive_files(files=fileobjs)

    def download_files(
        self, files: List[Tuple[Union[str, IO], str]], dst: str
    ):
        """Copy a list of files or dirctories from the file store to a given
        destination directory. The list of files contains tuples of relative
        file source and target path. The source path may reference files or
        directories.

        For the bucket store the source may also be a bytes buffer for an
        uploaded file that has been downloaded previously via the load_file
        method.

        Parameters
        ----------
        files: list((string or BytesIO, string))
            List of file source and target path. All path names are relative.
        dst: string
            Path to target directory on disk.

        Raises
        ------
        ValueError
        """
        for source, target in files:
            # The source may either reference an object in the bucket via its
            # key or be a bytes buffer that has previously been loaded.
            if isinstance(source, str):
                # Object keys are expected to be relative paths. If the source
                # string is an absolute path it is assumed that a file on disk
                # is referenced.
                if os.path.isabs(source):
                    util.copy_files(
                        files=[(source, target)],
                        target_dir=dst,
                        overwrite=False,
                        raise_error=True
                    )
                else:
                    downloads = download_list(
                        source=source,
                        target=target,
                        bucket=self.bucket
                    )
                    for key, target in downloads:
                        outfile = os.path.join(dst, target)
                        os.makedirs(os.path.dirname(outfile), exist_ok=True)
                        data = self.load_file(key)
                        with open(outfile, 'wb') as f:
                            f.write(data.read())
            else:
                outfile = os.path.join(dst, target)
                os.makedirs(os.path.dirname(outfile), exist_ok=True)
                with open(outfile, 'wb') as f:
                    f.write(source.read())

    def load_file(self, key: str) -> str:
        """Get a file object for the given key. Returns a buffer with the file
        content.

        Parameters
        ----------
        key: string
            Unique file key.

        Returns
        -------
        io.BytesIO

        Raises
        ------
        flowserv.error.UnknownFileError
        """
        # Load object into a new bytes buffer.
        data = BytesIO()
        try:
            self.bucket.download_fileobj(key, data)
        except botocore.exceptions.ClientError:
            raise err.UnknownFileError(key)
        # Ensure to reset the read pointer of the buffer before returning it.
        data.seek(0)
        return data

    def upload_file(self, file: Union[str, IO], dst: str) -> int:
        """Upload a given file object to the file store. The destination path
        is a relative path. The file may reference a file on the local file
        system or it is a file object (StringIO or BytesIO).

        Returns the size of the uploaded file on disk.

        Paramaters
        ----------
        file: string or io.BytesIO or io.StringIO
            The input file is either a FileObject (buffer) or a reference to a
            file on the local file system.
        dst: string
            Relative target path for the stored file.

        Returns
        -------
        int
        """
        # Depending on the type of the file parameter copy an input file or
        # write the contents of a file buffer object to disk.
        if isinstance(file, str):
            self.bucket.upload_file(file, dst)
            return os.stat(file).st_size
        if isinstance(file, StringIO):
            file.seek(0)
            file = BytesIO(bytes(file.read(), 'utf-8'))
        file.seek(0)
        f_size = file.getbuffer().nbytes
        self.bucket.upload_fileobj(file, dst)
        return f_size


# -- Helper Methods -----------------------------------------------------------

def download_list(
    source: str, target: str, bucket: B
) -> List[Tuple[str, str]]:
    """Create a list of objects that need to be downloaded based on the given
    source key. Returns a list of (key, path) where key is the key for the
    downloaded object and path is the relative target path.

    Parameters
    ----------
    source: string
        Query for object keys
    target: string
        Relative target (prefix) for all matched files.
    bucket: S3.bucket
        S3 bucket object.

    Returns
    -------
    list
    """
    result = list()
    # Ensure that if source ends with a '/' so does target and vice
    # versa.
    if source[-1] == '/' and target[-1] != '/':
        target = '{}/'.format(target)
    elif target[-1] == '/' and source[-1] != '/':
        source = '{}/'.format(source)
    # Find matching keys for each source in the file list.
    for key in find_keys(query=source, bucket=bucket):
        # Replace the source string at the beginning of the key
        # with the destination string (only for files in folders)
        if key == source:
            outfile = target
        else:
            outfile = '{}{}'.format(target, key[len(source):])
        result.append((key, outfile))
    return result


def find_keys(query: str, bucket: B) -> Set[str]:
    """Find all keys in a bucket that match the given query. The query may
    reference a single object of a folder of objects. In case of the latter
    we add all objects in the folder (recursively) to the returned key set.

    Parameters
    ----------
    query: string
        Query key
    bucket: S3.bucket
        S3 bucket object.

    Returns
    -------
    set
    """
    keyset = set()
    # Prefix for objects if the query references a directory.
    prefix = query if query[-1] == '/' else '{}/'.format(query)
    for obj in bucket.objects.filter(Prefix=query):
        if obj.key == query:
            keyset.add(obj.key)
        else:
            # Ensure thet the matched key is a file in a (sub-)folder
            # referenced by the query.
            if obj.key.startswith(prefix):
                keyset.add(obj.key)
    return keyset


def upload_dir(source: str, target: str, bucket: B):
    """Recursively upload all files in the given directory and its sub-
    directories to the given bucket.

    Parameters
    ----------
    src: string
        Path to the source directory.
    target: string
        Relative target path for uploaded files.
    bucket: S3.bucket
        S3 bucket object.
    """
    for filename in os.listdir(source):
        src = os.path.join(source, filename)
        dst = os.path.join(target, filename)
        if os.path.isfile(src):
            bucket.upload_file(src, dst)
        else:
            upload_dir(source=src, target=dst, bucket=bucket)
