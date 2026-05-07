package MiniDB.StorageEngine;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.nio.file.Path;

public class PageFile {
    private final Path filePath;

    public PageFile(Path filePath) {
        this.filePath = filePath;
    }

    //page count
    public int getPageCount()  {
        try{
            if(!Files.exists(filePath)) {
                throw new StorageException("File does not exist!");
            }
            long size = Files.size(filePath);
            if(size % Page.PAGE_SIZE !=0){
                throw new StorageException("Unexpected Mismatch in stored file size! ");
            }
            return (int)size/Page.PAGE_SIZE;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

    }

    public Page readPage(int pageNo) {
        try(RandomAccessFile raf = new RandomAccessFile(filePath.toFile() , "r")){
            long offset = (long) pageNo *Page.PAGE_SIZE;

            if(offset + Page.PAGE_SIZE > raf.length()){
                throw new StorageException("Page out of bounds!");
            }
            byte[] data = new byte[Page.PAGE_SIZE];
            raf.seek(offset);
            raf.readFully(data);
            return Page.wrap(data);


        } catch (FileNotFoundException e) {
            throw new StorageException("File does not exist!" ,e);
        } catch (IOException e) {
            throw new StorageException("Error reading file!" ,e);
        }
    }

    public void writePage(Page page, int pageNo) {
        try(RandomAccessFile raf = new RandomAccessFile(filePath.toFile(), "rw")){
            long offset = (long) pageNo *Page.PAGE_SIZE;
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
    public int appendPage(Page page) {
        int pageNo = getPageCount();
        writePage(page, pageNo);
        return pageNo;
    }
}
