package MiniDB.StorageEngine;

public record Slot(int offset, int length, boolean deleted) {
    public boolean isUsed(int slotId){
        return offset !=0||length !=0;
    }
}
