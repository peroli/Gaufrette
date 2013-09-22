<?php

namespace Gaufrette\Adapter;

use Aws\S3\S3Client as AmazonClient;
use Gaufrette\Adapter;

/**
 * Amazon S3 adapter
 *
 * @package Gaufrette
 * @author  Antoine Hérault <antoine.herault@gmail.com>
 * @author  Leszek Prabucki <leszek.prabucki@gmail.com>
 */
class AmazonS3 implements Adapter,
                          MetadataSupporter,
                          ListKeysAware
{
    protected $service;
    protected $bucket;
    protected $ensureBucket = false;
    protected $metadata;
    protected $options;

    public function __construct(AmazonClient $service, $bucket, $options = array())
    {
        $this->service = $service;
        $this->bucket  = $bucket;
        $this->options = array_replace_recursive(
            array(
                'directory' => '', 'create' => false, 'region' => '',
                'acl' => ''
            ),
            $options
        );
    }

    /**
     * Set the acl used when writing files
     *
     * @param string $acl
     */
    public function setAcl($acl)
    {
        $this->options['acl'] = $acl;
    }

    /**
     * Get the acl used when writing files
     *
     * @return string
     */
    public function getAcl()
    {
        return $this->options['acl'];
    }

    /**
     * Set the base directory the user will have access to
     *
     * @param string $directory
     */
    public function setDirectory($directory)
    {
        $this->options['directory'] = $directory;
    }

    /**
     * Get the directory the user has access to
     *
     * @return string
     */
    public function getDirectory()
    {
        return $this->options['directory'];
    }

    /**
     * {@inheritDoc}
     */
    public function setMetadata($key, $metadata)
    {
        $path = $this->computePath($key);

        $this->metadata[$path] = $metadata;
    }

    /**
     * {@inheritDoc}
     */
    public function getMetadata($key)
    {
        $path = $this->computePath($key);

        return isset($this->metadata[$path]) ? $this->metadata[$path] : array();
    }

    /**
     * {@inheritDoc}
     */
    public function read($key)
    {
        $this->ensureBucketExists();
        $options = [
            'Bucket' => $this->bucket,
            'Key' => $this->computePath($key)
        ];
        $options = array_merge($options, $this->getMetadata($key));
        $response = $this->service->getObject($options);
        return $response->get('Body');
    }

    /**
     * {@inheritDoc}
     */
    public function rename($sourceKey, $targetKey)
    {
        $this->ensureBucketExists();
        $options = [
            'Bucket'   => $this->bucket,
            'CopySource' => $this->bucket . '/'. $this->computePath($sourceKey),
            'Key' => $this->computePath($targetKey)
        ];
        $options = array_merge($options, $this->getMetadata($sourceKey));
        $response = $this->service->copyObject($options);
        $this->delete($sourceKey);

        return $response;
    }

    /**
     * {@inheritDoc}
     */
    public function write($key, $content)
    {
        $this->ensureBucketExists();

        $opt = array_replace_recursive(
            array('ACL'  => $this->options['acl']),
            $this->getMetadata($key),
            array('Body' => $content)
        );

        $options = array_merge($opt, [
            'Bucket' => $this->bucket,
            'Key' => $this->computePath($key),
        ]);

        $response = $this->service->putObject($options);
        return $response;
    }

    /**
     * {@inheritDoc}
     */
    public function exists($key)
    {
        $this->ensureBucketExists();

        return $this->service->doesObjectExist(
            $this->bucket,
            $this->computePath($key)
        );
    }

    /**
     * {@inheritDoc}
     */
    public function mtime($key)
    {
        $this->ensureBucketExists();
        $options = [
            'Bucket' => $this->bucket,
            'Key' => $this->computePath($key),
        ];
        $options = array_merge($options, $this->getMetadata($key));
        $response = $this->service->headObject($options);
        $lastModified = $response->get('LastModified') || false;

        return $lastModified;
    }

    /**
     * {@inheritDoc}
     */
    public function keys()
    {
        $this->ensureBucketExists();

        $list = $this->service->listObjects(['Bucket' => $this->bucket]);

        $keys = array();
        foreach ($list as $file) {
            if ('.' !== dirname($file)) {
                $keys[] = dirname($file);
            }
            $keys[] = $file;
        }
        sort($keys);

        return $keys;
    }

    /**
     * {@inheritDoc}
     */
    public function listKeys($prefix = '') {
        die('To write list keys method');
    }

    /**
     * {@inheritDoc}
     */
    public function delete($key)
    {
        $this->ensureBucketExists();
        $options = [
            'Bucket' => $this->bucket,
            'Key' => $this->computePath($key),
        ];
        $options = array_merge($options, $this->getMetadata($key));
        $response = $this->service->deleteObject($options);

        return $response;
    }

    /**
     * {@inheritDoc}
     */
    public function isDirectory($key)
    {
        if ($this->exists($key.'/')) {
            return true;
        }

        return false;
    }

    /**
     * Ensures the specified bucket exists. If the bucket does not exists
     * and the create parameter is set to true, it will try to create the
     * bucket
     *
     * @throws \RuntimeException if the bucket does not exists or could not be
     *                          created
     */
    private function ensureBucketExists()
    {
        if ($this->ensureBucket) {
            return;
        }

        if (isset($this->options['region'])) {
            $this->service->setRegion($this->options['region']);
        }

        if ($this->service->doesBucketExist($this->bucket)) {
            $this->ensureBucket = true;

            return;
        }

        if (!$this->options['create']) {
            throw new \RuntimeException(sprintf(
                'The configured bucket "%s" does not exist.',
                $this->bucket
            ));
        }

        $response = $this->service->createBucket([
            'Bucket' => $this->bucket,
            'LocationConstraint' => $this->options['region']
        ]);

        $this->ensureBucket = true;
    }

    /**
     * Computes the path for the specified key taking the bucket in account
     *
     * @param string $key The key for which to compute the path
     *
     * @return string
     */
    private function computePath($key)
    {
        $directory = $this->getDirectory();
        if (null === $directory || '' === $directory) {
            return $key;
        }

        return sprintf('%s/%s', $directory, $key);
    }
}
