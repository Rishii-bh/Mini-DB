package MiniDB.Index;

import java.util.ArrayList;
import java.util.List;

public class IndexPage {
    public static final int INDEX_PAGE_SIZE = 4096;
    private static final int HEADER_SIZE = 16;
    private static final int PAGE_TYPE_OFFSET = 0;
    private static final int PAGE_ENTRY_COUNT_OFFSET = 4;
    private static final int USED_SPACE_OFFSET = 8;
    private static final int RESERVED_SPACE_OFFSET = 12;
    private static final int PAGE_TYPE_INDEX_DATA =1;
    private static final int FIXED_RECORDID_LENGTH = 12;

    private final byte[] data;

    public IndexPage(byte[] data) {
        if(data == null || data.length < INDEX_PAGE_SIZE){
            throw new IndexException("Index data is null or unspecified value");
        }
        this.data = data;
    }

    public static IndexPage createIndexPage(){
       byte[] data = new byte[INDEX_PAGE_SIZE];
       IndexPage indexPage = new IndexPage(data);
       indexPage.setPageType(PAGE_TYPE_INDEX_DATA);
       indexPage.setUsedSpace(HEADER_SIZE);
       indexPage.setEntryCount(0);
       return indexPage;

    }
    public byte[] getData() {
        return data;
    }
    public static IndexPage wrap(byte[] data) {
        return new IndexPage(data);
    }

    private void setPageType(int pageType) {
        writeInt(PAGE_TYPE_OFFSET, pageType);
    }

    public int getPageType() {
        return readInt(PAGE_TYPE_OFFSET);
    }

    private void setEntryCount(int entryCount) {
        writeInt(PAGE_ENTRY_COUNT_OFFSET, entryCount);
    }

    public int getEntryCount() {
        return readInt(PAGE_ENTRY_COUNT_OFFSET);
    }

    private void setUsedSpace(int usedSpace) {
        writeInt(USED_SPACE_OFFSET, usedSpace);
    }

    public int getUsedSpace() {
        return readInt(USED_SPACE_OFFSET);
    }

    public int remainingSpace() {
        return INDEX_PAGE_SIZE - getUsedSpace();
    }

    public boolean tryInsert(byte[] indexBytes) {
        if(indexBytes == null){
            throw new IndexException("Index bytes is null !");
        }
        if(indexBytes.length >remainingSpace()){
            return false;
        }
        int offset = getUsedSpace();
        System.arraycopy(indexBytes, 0, data, offset, indexBytes.length);
        setUsedSpace(offset + indexBytes.length);
        int entryCount = getEntryCount();
        setEntryCount(entryCount+1);
        return true;
    }

    public List<byte[]> getIndexes(){
        List<byte[]> indexes = new ArrayList<byte[]>();
        if(getUsedSpace() == HEADER_SIZE){
            return List.of();
        }
        int indexCount = getEntryCount();
        int offset = HEADER_SIZE;
        for(int index = 0; index < indexCount && offset < INDEX_PAGE_SIZE; index++){
            int keyLength = readInt(offset);
            int indexLength = keyLength + FIXED_RECORDID_LENGTH;
            byte[] indexBytes = new byte[indexLength];
            System.arraycopy(data, offset, indexBytes, 0, indexLength);
            indexes.add(indexBytes);
            offset += indexLength;
        }
        return indexes;
    }


    private void writeInt(int offset, int value) {
        data[offset]     = (byte) (value >>> 24);
        data[offset + 1] = (byte) (value >>> 16);
        data[offset + 2] = (byte) (value >>> 8);
        data[offset + 3] = (byte) value;
    }

    private int readInt(int offset) {
        return ((data[offset] & 0xFF) << 24)
                | ((data[offset + 1] & 0xFF) << 16)
                | ((data[offset + 2] & 0xFF) << 8)
                |  (data[offset + 3] & 0xFF);
    }




}
