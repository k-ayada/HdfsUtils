package pub.ayada.scala.hdfsutils

import java.io.{ File, InputStream, FileInputStream, OutputStream }

import org.apache.hadoop.fs.{ Path, FileSystem, FSDataInputStream }

import org.apache.hadoop.io.IOUtils
import org.apache.hadoop.conf.Configuration
import java.io.FileOutputStream
import org.apache.hadoop.fs.FileUtil

object HdfsFileIO {

    def moveFlLocal2Hdfs(HDPClientConf: Configuration, inFilePath: String, outputPath: String): Unit = {
        val fs: FileSystem = FileSystem.get(HDPClientConf)
        val dir: File = new File(inFilePath)
        val files: Array[File] = dir.listFiles()
        for (file <- files) {
            val inFileStream: FileInputStream = new FileInputStream(file)
            val os: OutputStream = fs.create(new Path(outputPath))
            IOUtils.copyBytes(inFileStream, os, HDPClientConf)
            file.delete()
        }
    }
    def cpFlLocal2Hdfs(HDPClientConf: Configuration, inFilePath: String, outputPath: String): Unit = {
        val fs: FileSystem = FileSystem.get(HDPClientConf)
        val dir: File = new File(inFilePath)
        val files: Array[File] = dir.listFiles()
        for (file <- files) {
            val inFileStream: FileInputStream = new FileInputStream(file)
            val os: OutputStream = fs.create(new Path(outputPath))
            IOUtils.copyBytes(inFileStream, os, HDPClientConf)
        }
    }

    def getHdfsFlInputStream(inFilePath: String): InputStream = {
        getHdfsFlInputStream(new Configuration(), inFilePath)
    }

    def getHdfsFlInputStream(HDPClientConf: Configuration, inFilePath: String): InputStream = {
        val fs: FileSystem = FileSystem.get(HDPClientConf)
        val fl = if (inFilePath.startsWith("hdfs://")) inFilePath else { "hdfs://" + inFilePath }
        val fis: FSDataInputStream = fs.open(new Path(inFilePath))
        fis.getWrappedStream()
    }

    def moveFlHdfs2Hdfs(HDPClientConf: Configuration, frmPath: String, toPath: String): Unit = {
        val fs: FileSystem = FileSystem.get(HDPClientConf)
        fs.rename(new Path(frmPath), new Path(toPath));
    }

    def cpFlHdfs2Local(HDPClientConf: Configuration, inFilePath: String, outputPath: String, bufferSize: Int): Unit = {
        val inStream: InputStream = getHdfsFlInputStream(HDPClientConf, inFilePath)
        val outStream: OutputStream = new FileOutputStream(outputPath);
        val buffer = new Array[Byte](bufferSize)

        def recurseCopy() {
            val len = inStream.read(buffer)
            if (len > 0) {
                outStream.write(buffer.take(len))
                recurseCopy();
            }
        }
        recurseCopy();
    }
    def moveFlHdfs2Local(HDPClientConf: Configuration, inFilePath: String, outputPath: String, bufferSize: Int): Unit = {
        cpFlHdfs2Local(HDPClientConf, inFilePath, outputPath, bufferSize)
        val fs: FileSystem = FileSystem.get(HDPClientConf)
        fs.delete(new Path(inFilePath), true);
    }

    def mergeHdfsFileParts(HDPClientConf: Configuration, inFilePath: String, outputPath: String): Boolean = {
        val fs: FileSystem = FileSystem.get(HDPClientConf)
        return FileUtil.copyMerge(fs, new Path(inFilePath), fs, new Path(outputPath), false, HDPClientConf, null);
    }

}
