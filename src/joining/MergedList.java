package joining;

import java.util.*;

public class MergedList<T> extends AbstractList<T>
{
    private final List<List<T>> list0;

    private int nrElement = 0;

    public MergedList()
    {
        this.list0 = new ArrayList<>();
    }

    public MergedList(int size)
    {
        this.list0 = new ArrayList<>(size);
        this.nrElement = size;
    }

    public void add(List<T> list) {
        if (list.size() > 0) {
            list0.add(list);
            nrElement += 1;
        }
    }

    @Override
    public T get(int index)
    {
        int i = 0;
        for (List<T> l: list0) {
            if (i + l.size() > index) {
                return l.get(index - i);
            }
            i += l.size();
        }
        return list0.get(nrElement - 1).get(index - i);
    }

    @Override
    public Iterator<T> iterator()
    {
        return new Iterator<T>()
        {
            private Iterator<T> current = list0.get(0).iterator();
            private int pos = 0;

            @Override
            public boolean hasNext()
            {
//                System.out.println("1:" + (pos < nrElement - 1));
//                System.out.println("2:" + current.hasNext());
                return current != null;
            }

            @Override
            public T next()
            {
                T result = current.next();
                if (!current.hasNext())
                {
                    if (pos < nrElement - 1)
                    {
                        pos += 1;
                        current = list0.get(pos).iterator();
                    }
                    else
                    {
                        current = null;
                    }
                }
                return result;
            }
        };
    }

    @Override
    public int size()
    {
        return list0.stream().mapToInt(List::size).sum();
    }

    public static void main(String[] args) {
        List<int[]> a = new ArrayList<>();
        a.add(new int[]{1, 3, 4});
        a.add(new int[]{10, 30, 40});
        a.add(new int[]{23, 33, 34});
        List<int[]> b = new ArrayList<>();
        b.add(new int[]{1, 3, 4});
        List<int[]> c = new ArrayList<>();
        c.add(new int[]{140, 330, 430});
        c.add(new int[]{233, 333, 334});
        List<int[]> d = new ArrayList<>();
        MergedList<int []> m = new MergedList<>();
        m.add(a);
        m.add(b);
        m.add(c);
        m.add(d);
        for(int[] t:m) {
            System.out.println(Arrays.toString(t));
        }
    }
}
