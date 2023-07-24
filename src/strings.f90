module strings
  implicit none
  private 

  type, public :: string_t
    character(:), allocatable :: str
  contains
    ! Public methods
    procedure, pass(lhs) :: equals
    generic, public :: operator(==) => equals 
  end type
contains
  
  elemental function equals(lhs, rhs) result(res)
    class(string_t), intent(in) :: lhs, rhs
    logical :: res

    res = lhs%str == rhs%str
  end function equals
end module strings