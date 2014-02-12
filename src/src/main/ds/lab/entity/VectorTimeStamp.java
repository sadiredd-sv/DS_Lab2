package ds.lab.entity;

import java.util.ArrayList;
import java.util.List;

public class VectorTimeStamp extends TimeStamp implements Cloneable {
	private List<Integer> mVector;
	
	private int mProcessIndex;
	
	private int mNumProcesses;
	
	public VectorTimeStamp(int startFrom, int processIndex, int numProcesses) {
		mProcessIndex = processIndex;
		mNumProcesses = numProcesses;
		
		mVector = new ArrayList<Integer>(numProcesses);
		for (int i = 0; i < numProcesses; ++i) {
			mVector.add(startFrom);
		}
	}
	
	public VectorTimeStamp(List<Integer> vector, int processIndex, int numProcesses) {
		mVector = new ArrayList<Integer>(numProcesses);
		mVector.addAll(vector);
		
		mProcessIndex = processIndex;
		mNumProcesses = numProcesses;
	}
	
	public VectorTimeStamp next(int diff) {
		VectorTimeStamp nextTimeStamp = new VectorTimeStamp(mVector, mProcessIndex, mNumProcesses);
		nextTimeStamp.mVector.set(mProcessIndex, nextTimeStamp.mVector.get(mProcessIndex) + diff);
		return nextTimeStamp;
	}
	
	public VectorTimeStamp sync(VectorTimeStamp other) {
		List<Integer> va = mVector;
		List<Integer> vb = other.mVector;
		
		List<Integer> vc = new ArrayList<Integer>(mNumProcesses);
		for (int i = 0; i < mNumProcesses; ++i) {
			vc.add(Math.max(va.get(i), vb.get(i)));
		}
		return new VectorTimeStamp(vc, mProcessIndex, mNumProcesses);
	}
	
	@Override
	public int compareTo(TimeStamp other) {
		//		ta = tb iff for all i, ta[i] = tb[i]
		//		ta <> tb iff for any i, ta[i] <> tb[i]
		//		ta ² tb iff for all i, ta[i] ² tb[i] (each one equal or less)
		//		ta < tb iff ta ² tb and ta <> tb (some (but not all) equal, some less)
		List<Integer> va = mVector;
		List<Integer> vb = ((VectorTimeStamp)other).mVector;
		
		if (va.equals(vb)) {
			return 0;
		}
		if (isLessThanOrEqualTo(va, vb)) {
			return -1;
		}
		if (isLessThanOrEqualTo(vb, va)) {
			return 1;
		}
		return 0;
	}
	
	private boolean isLessThanOrEqualTo(List<Integer> va, List<Integer> vb) {
		for (int i = 0; i < va.size(); ++i) {
			int a = va.get(i);
			int b = vb.get(i);
			if (b > a) {
				return false;
			}
		}
		return true;
	}

    private static final long serialVersionUID = 4758470450663133929L;

	@Override
	public Object getValue() {
		return mVector;
	}
	
	public int get(int i) {
		return mVector.get(i);
	}
	
	public void set(int i, int v) {
		mVector.set(i, v);
	}
	
	@Override
	public void setValue(Object value) {
		mVector.clear();
		mVector.addAll((List<Integer>)value);
	}
	
	@Override
	public String toString() {
		return String.format("<" + mVector.toString() + '>');
	}

	public int getProcessIndex() {
		return mProcessIndex;
	}

	public int getNumProcesses() {
		return mNumProcesses;
	
	
	}
	
	public VectorTimeStamp clone() {
		return new VectorTimeStamp(mVector, mProcessIndex, mNumProcesses);
	}
	
	@Override
	public int hashCode() {
		int h = 0;
		for (int i : mVector) {
			h ^= i;
		}
		return h;
	}
	
	@Override
	public boolean equals(Object o) {
		if (!(o instanceof VectorTimeStamp)) {
			return false;
		}
		return mVector.equals(((VectorTimeStamp) o).mVector);
	}
}
