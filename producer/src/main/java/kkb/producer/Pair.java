package kkb.producer;
import java.util.Objects;

// C++에 있던 pair가 없는 것 같아서 대충 구현
public class Pair<F, S> {
    public final F first;
    public final S second;
   
    public Pair(F first, S second) {
        this.first = first;
        this.second = second;
    }
}