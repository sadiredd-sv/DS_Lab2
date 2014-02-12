package ds.lab.entity.test;

import static org.junit.Assert.*;

import java.util.List;

import org.junit.Test;

import ds.lab.entity.VectorTimeStamp;

public class VectorTimeStampTest {

	@SuppressWarnings("unchecked")
	@Test
	public void testInitialTimeStamp() {
		VectorTimeStamp vts = new VectorTimeStamp(0, 0, 3);
		List<Integer> vector = (List<Integer>)vts.getValue();
		for (int i = 0; i < vector.size(); ++i) {
			assertSame(vector.get(i), 0);
		}
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testNextTimeStamp() {
		VectorTimeStamp vts = new VectorTimeStamp(0, 0, 3);
		vts = vts.next(1);
		List<Integer> vector = (List<Integer>)vts.getValue();
		assertSame(vector.get(0), 1);
		assertSame(vector.get(1), 0);
		assertSame(vector.get(2), 0);
		
		vts = vts.next(1);
		vector = (List<Integer>)vts.getValue();
		assertSame(vector.get(0), 2);
		assertSame(vector.get(1), 0);
		assertSame(vector.get(2), 0);
	}
	
	@SuppressWarnings("unchecked")
	@Test
	public void testAdjustTimeStamp() {
		VectorTimeStamp vtsa = new VectorTimeStamp(0, 0, 3).next(1);
		
		VectorTimeStamp vtsb = new VectorTimeStamp(0, 1, 3).next(1).next(1);
		
		VectorTimeStamp vtsc = vtsa.sync(vtsb);
		List<Integer> vc = (List<Integer>)vtsc.getValue();
		assertSame(vc.get(0), 1);
		assertSame(vc.get(1), 2);
		assertSame(vc.get(2), 0);
	}

}
