import MiniDB.StorageEngine.Page;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class PageTests {
    @Test
    void insertRowBytesIntoPage(){
        Page page = Page.createEmptyPage();
        byte[] row = {1,2,3,4};
        int slotId= page.tryInsert(row);

        assertEquals(0,slotId);
        assertEquals(1,page.getSlotCount());
        assertArrayEquals(row,page.getRowBytes(slotId));
    }

    @Test
    void persistanceTests() {
        Page page = Page.createEmptyPage();
        byte[] row = {1,2,3,4};
        int slotId= page.tryInsert(row);
        byte[] persistedData = page.getData();
        Page persistedPage = Page.wrap(persistedData);

        assertEquals(1,persistedPage.getSlotCount());
        assertArrayEquals(page.getRowBytes(slotId), persistedPage.getRowBytes(slotId));
        assertEquals(persistedPage.getFreeSpaceEnd(),page.getFreeSpaceEnd());
    }
}
