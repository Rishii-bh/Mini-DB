package MiniDB.StorageEngine;

import java.util.Optional;

public class Page {
    public static final int PAGE_SIZE = 4096;
    private static final int SLOT_COUNT_OFFSET = 0;
    private static final int FREE_SPACE_END_OFFSET= 2;
    private static final int MAX_REUSABLE_LENGTH_OFFSET = 4;
    private static final int HEADER_SIZE=16;
    private static final int SLOT_SIZE =5;
    private final byte[] data;
//This Page basically acts as a wrapper for 4096 bytes. Header space is 16Bytes reserved and the rest is free
    //Slot data starts from where header ends and row data starts from the end
    //so both sorta meet at the middle. The byte array is the source of truth everything is read from there
    //slot access is O(1) since slot_size is fixed
    //each slot stores the data of the row offset and its length so row access also becomes O(1)
    public Page(byte[] data) {
        if (data == null) {
            throw new StorageException("Page data cannot be null");
        }

        if (data.length != PAGE_SIZE) {
            throw new StorageException("Page must be exactly " + PAGE_SIZE + " bytes");
        }

        this.data = data;
    }
    public static Page createEmptyPage() {
        byte[] data = new byte[PAGE_SIZE];
        Page page = new Page(data);
        page.setSlotCount(0);
        page.setFreeSpaceEnd(PAGE_SIZE);
        page.setMaxReusableLength(0);
        return page;
    }

    public static Page wrap(byte[] data) {
        return new Page(data);
    }

    public byte[] getData() {
        return data;
    }

    public int getSlotCount() {
        return readShort(SLOT_COUNT_OFFSET);
    }
    public void setSlotCount(int slotCount) {
        writeShort(SLOT_COUNT_OFFSET,slotCount);
    }
    public void incrementSlotCount() {
         setSlotCount(getSlotCount()+1);
    }

    public int getFreeSpaceEnd() {
        return readShort(FREE_SPACE_END_OFFSET);
    }
    public void setFreeSpaceEnd(int freeSpaceEnd) {
        writeShort(FREE_SPACE_END_OFFSET,freeSpaceEnd);
    }

    public int getFreeSpaceStart() {
        return HEADER_SIZE + getSlotCount()*SLOT_SIZE;
    }

    public int getAvailableSpace() {
        return getFreeSpaceEnd() - getFreeSpaceStart();
    }

    public int getMaxReusableLength() {
        return readShort(MAX_REUSABLE_LENGTH_OFFSET);
    }

    public void setMaxReusableLength(int maxReusableLength) {
        writeShort(MAX_REUSABLE_LENGTH_OFFSET,maxReusableLength);
    }

    private int slotOffset(int slotId) {
        return HEADER_SIZE + slotId * SLOT_SIZE;
    }

    public void writeSlot(int slotId, int rowOffSet, int length , boolean deleted) {
        int pos = slotOffset(slotId);
        writeShort(pos, rowOffSet);
        writeShort(pos+2, length);
        writeBoolean(pos+4,deleted);
    }

    public Slot readSlot(int slotId) {
        if(slotId<0 || slotId>=getSlotCount()) {
            throw new StorageException("Invalid slot ID!");
        }
        int pos = slotOffset(slotId);
        int rowOffSet = readShort(pos);
        int length = readShort(pos+2);
        boolean deleted = readDeletedFlag(pos+4);
        return new Slot(rowOffSet, length,deleted);
    }

    private int readShort(int offset) {
        return ((data[offset] & 0xFF) << 8)
                | (data[offset + 1] & 0xFF);
    }

    private void writeShort(int offset, int value) {
        if (value < 0 || value > 65535) {
            throw new IllegalArgumentException("Value does not fit in unsigned short: " + value);
        }

        data[offset] = (byte) ((value >>> 8) & 0xFF);
        data[offset + 1] = (byte) (value & 0xFF);
    }

    private boolean readDeletedFlag(int offset) {
        return data[offset] != 0;
    }

    private void writeBoolean(int offset , boolean value) {
        data[offset] = (byte) (value ? 1 : 0);
    }

    public int tryInsert(byte[] rowBytes) {
        if(rowBytes == null){
            throw new StorageException("Row bytes cannot be null!");
        }
        if(rowBytes.length > getAvailableSpace()-SLOT_SIZE){
            return -1;
        }
        int slotId = getSlotCount();
        incrementSlotCount();
        int rowOffSet = getFreeSpaceEnd()- rowBytes.length;
        writeSlot(slotId, rowOffSet, rowBytes.length , false);
        System.arraycopy(rowBytes, 0, data, rowOffSet , rowBytes.length);
        setFreeSpaceEnd(getFreeSpaceEnd()-rowBytes.length);
        return slotId;
    }
    public boolean reusableSlotExists(int length) {
        return getMaxReusableLength()>=length;
    }

    public int tryInsertInDeletedSlot(byte[] rowBytes) {
        if(rowBytes == null){
            throw new StorageException("Row bytes cannot be null!");
        }
        int slotCount = getSlotCount();
        for(int slotId=0; slotId<slotCount; slotId++) {
            Slot slot = readSlot(slotId);
            if(slot.deleted() && slot.length()>= rowBytes.length) {
                writeSlot(slotId, slot.offset(), rowBytes.length , false);
                System.arraycopy(rowBytes,0,data, slot.offset(), rowBytes.length);
                computeMaxReusableSlotLength();
                return slotId;
            }
        }
        return -1;
    }

    private void computeMaxReusableSlotLength(){
        int slotCount = getSlotCount();
        int maxReusableLength =0;
        for(int slotId=0; slotId<slotCount; slotId++) {
            Slot slot = readSlot(slotId);
            if(slot.deleted() && slot.length()>= maxReusableLength) {
                maxReusableLength = slot.length();
            }
        }
        setMaxReusableLength(maxReusableLength);
    }

    public byte[] getRowBytes(int slotId) {
        if(slotId<0 || slotId>=getSlotCount()) {
            throw new StorageException("Invalid slot ID!");
        }
        Slot slot = readSlot(slotId);
        byte[] rowBytes = new byte[slot.length()];
        System.arraycopy(data,slot.offset(),rowBytes,0,slot.length());
        return rowBytes;
    }

    public Optional<byte[]> readLiveRowBytes(int slotId) {
        if(isDeletedSlot(slotId)) {
            return Optional.empty();
        }
        return Optional.of(getRowBytes(slotId));
    }

    public void markSlotAsDeleted(int slotId) {
        Slot slot = readSlot(slotId);
        if(slot == null) {
            throw new StorageException("Invalid slot ID!");
        }
        if(slot.deleted()){
            return;
        }
        writeSlot(slotId,slot.offset(),slot.length(),true);
        if(slot.length() > getMaxReusableLength()){
            setMaxReusableLength(slot.length());
        }
    }

    public boolean isDeletedSlot(int slotId) {
        Slot slot = readSlot(slotId);
        if(slot == null) {
            throw new StorageException("Invalid slot ID!");
        }
        return slot.deleted();
    }
}
