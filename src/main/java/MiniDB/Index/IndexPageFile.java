package MiniDB.Index;

import MiniDB.StorageEngine.Page;
import MiniDB.StorageEngine.StorageException;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

public class IndexPageFile {
    private final Path filePath;

    public IndexPageFile(Path filePath) {
        this.filePath = filePath;
    }
    public int getPageCount()  {
        try{
            if(!Files.exists(filePath)) {
                throw new StorageException("File does not exist!");
            }
            long size = Files.size(filePath);
            if(size % IndexPage.INDEX_PAGE_SIZE !=0){
                throw new StorageException("Unexpected Mismatch in stored file size! ");
            }
            return (int)size/IndexPage.INDEX_PAGE_SIZE;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public IndexPage readPage(int pageNo) {
        try(RandomAccessFile raf = new RandomAccessFile(filePath.toFile() , "r")){
            long offset = (long) pageNo *IndexPage.INDEX_PAGE_SIZE;

            if(offset + IndexPage.INDEX_PAGE_SIZE > raf.length()){
                throw new StorageException("Page out of bounds!");
            }
            byte[] data = new byte[IndexPage.INDEX_PAGE_SIZE];
            raf.seek(offset);
            raf.readFully(data);
            return IndexPage.wrap(data);


        } catch (FileNotFoundException e) {
            throw new StorageException("File does not exist!" ,e);
        } catch (IOException e) {
            throw new StorageException("Error reading file!" ,e);
        }
    }

    public void writePage(IndexPage page, int pageNo) {
        try(RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")){
            long offset = (long) pageNo *IndexPage.INDEX_PAGE_SIZE;
            if(offset>raf.length()){
                throw new StorageException("Page out of bounds!");
            }
            raf.seek(offset);
            raf.write(page.getData());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
    //appends page at the end of file
    public int appendPage(IndexPage page) {
        int pageNo = getPageCount();
        writePage(page, pageNo);
        return pageNo;
    }

    public void truncate() {
        try(RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")){
            raf.setLength(0);
        } catch (FileNotFoundException e) {
            throw new StorageException("Table Path does not exist!" ,e);
        } catch (IOException e) {
            throw new StorageException("Error truncating file!" ,e);
        }
    }
}
